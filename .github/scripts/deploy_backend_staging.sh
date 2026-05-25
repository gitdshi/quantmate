#!/bin/sh

set -eu

cd /opt/quantmate

ENV_FILE=.env
EXAMPLE_FILE=.env.example
INCOMING_EXAMPLE_FILE=.env.example.incoming
DEPLOY_BACKEND_SCRIPT=deploy_backend_staging.sh
DEPLOY_PORTAL_SCRIPT=deploy_portal_staging.sh
IMAGE_SERVICES="api worker paper-runtime datasync datasync-backfill rdagent-service"
POST_API_SERVICES="worker paper-runtime datasync datasync-backfill rdagent-service"
ALL_SERVICES="api worker paper-runtime datasync datasync-backfill rdagent-service"
DISK_USAGE_THRESHOLD_PERCENT="80"

cleanup_staging_directory() {
  echo "==> Removing stale source/artifact files from staging root"

  find . -mindepth 1 -maxdepth 1 \
    ! -name '.env' \
    ! -name '.env.example' \
    ! -name 'docker-compose.staging.yml' \
    ! -name 'nginx' \
    ! -name '.github' \
    -exec rm -rf {} +

  mkdir -p nginx .github/scripts

  find nginx -mindepth 1 -maxdepth 1 \
    ! -name 'staging.conf' \
    -exec rm -rf {} +

  find .github -mindepth 1 -maxdepth 1 \
    ! -name 'scripts' \
    -exec rm -rf {} +

  mkdir -p .github/scripts
  find .github/scripts -mindepth 1 -maxdepth 1 \
    ! -name 'merge_env_from_example.py' \
    ! -name "$DEPLOY_BACKEND_SCRIPT" \
    ! -name "$DEPLOY_PORTAL_SCRIPT" \
    -exec rm -rf {} +
}

rebuild_env_from_example_if_needed() {
  if [ ! -f "$INCOMING_EXAMPLE_FILE" ]; then
    echo "==> No incoming .env.example provided; keeping existing env files"
    return
  fi

  example_changed=0
  if [ ! -f "$EXAMPLE_FILE" ] || ! cmp -s "$EXAMPLE_FILE" "$INCOMING_EXAMPLE_FILE"; then
    example_changed=1
  fi

  if [ "$example_changed" -eq 1 ] || [ ! -f "$ENV_FILE" ]; then
    echo "==> Regenerating .env from incoming .env.example"
    python3 .github/scripts/merge_env_from_example.py \
      "$INCOMING_EXAMPLE_FILE" "$ENV_FILE" "$ENV_FILE.merged"
    mv "$ENV_FILE.merged" "$ENV_FILE"
  else
    echo "==> .env.example unchanged; keeping existing .env"
  fi

  mv "$INCOMING_EXAMPLE_FILE" "$EXAMPLE_FILE"
}

upsert_env_key() {
  key="$1"
  value="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    printf "\n%s=%s\n" "$key" "$value" >> "$ENV_FILE"
  fi
}

ensure_env_key() {
  key="$1"
  value="$2"
  if ! grep -q "^${key}=" "$ENV_FILE"; then
    printf "\n%s=%s\n" "$key" "$value" >> "$ENV_FILE"
  fi
}

read_env_key() {
  key="$1"
  if [ ! -f "$ENV_FILE" ]; then
    return
  fi
  grep "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2-
}

require_env_key() {
  key="$1"
  if ! grep -q "^${key}=.\+" "$ENV_FILE"; then
    echo "Required key $key is missing or empty in $ENV_FILE"
    exit 1
  fi
}

print_disk_diagnostics() {
  echo "==> Filesystem usage"
  df -h / || true
  echo "==> Docker disk usage"
  docker system df || true
}

root_usage_percent() {
  df -P / | awk 'NR == 2 {gsub(/%/, "", $5); print $5}'
}

cleanup_docker_artifacts() {
  echo "==> Pruning unused Docker data to recover disk space"
  docker system prune -af || true
  docker builder prune -af || true
  print_disk_diagnostics
}

cleanup_if_root_usage_high() {
  reason="$1"
  usage=$(root_usage_percent)
  if [ -n "$usage" ] && [ "$usage" -ge "$DISK_USAGE_THRESHOLD_PERCENT" ]; then
    echo "==> Root filesystem usage is ${usage}% (${reason}); running Docker cleanup"
    cleanup_docker_artifacts
  else
    echo "==> Root filesystem usage is ${usage:-unknown}% (${reason}); cleanup not needed"
  fi
}

cleanup_repo_images() {
  repo="$1"
  keep_ref="$2"
  label="$3"
  image_refs=$(docker image ls "$repo" --format '{{.Repository}}:{{.Tag}}' | awk 'NF && $0 !~ /:<none>$/')

  if [ -z "$image_refs" ]; then
    echo "==> No local $label images found for cleanup"
    return
  fi

  current_images=$(docker ps -a --format '{{.Image}}')
  echo "==> Cleaning unused $label images (keeping $keep_ref)"
  for image_ref in $image_refs; do
    if [ "$image_ref" = "$keep_ref" ]; then
      continue
    fi
    if printf '%s\n' "$current_images" | grep -Fxq "$image_ref"; then
      echo "Keeping image still referenced by a container: $image_ref"
      continue
    fi
    echo "Removing unused image: $image_ref"
    docker image rm "$image_ref" >/dev/null 2>&1 || echo "Failed to remove $image_ref; skipping"
  done
}

cleanup_managed_images() {
  cleanup_repo_images "ghcr.io/$GITHUB_OWNER/quantmate-api" "ghcr.io/$GITHUB_OWNER/quantmate-api:$IMAGE_TAG" "quantmate-api"
  current_portal_tag=$(read_env_key PORTAL_IMAGE_TAG)
  if [ -n "$current_portal_tag" ]; then
    cleanup_repo_images "ghcr.io/$GITHUB_OWNER/quantmate-portal" "ghcr.io/$GITHUB_OWNER/quantmate-portal:$current_portal_tag" "quantmate-portal"
  fi
}

restart_nginx() {
  echo "==> Restarting nginx to refresh upstream container IPs"
  docker restart quantmate_nginx >/dev/null
}

restart_post_api_services() {
  GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
    docker compose -f docker-compose.staging.yml up -d $POST_API_SERVICES
}

restart_services() {
  echo "==> Starting api first so migrations land before datasync services restart"
  GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
    docker compose -f docker-compose.staging.yml up -d api

  echo "==> Waiting for direct API health before starting worker and datasync"
  api_ok=0
  for i in $(seq 1 24); do
    if curl -sf http://localhost:8000/health >/dev/null; then
      api_ok=1
      break
    fi
    echo "Direct API health retry $i/24..."
    sleep 5
  done

  if [ "$api_ok" -ne 1 ]; then
    echo "API did not become healthy in time after api-first restart"
    docker compose -f docker-compose.staging.yml ps
    docker compose -f docker-compose.staging.yml logs --tail=200 api
    exit 1
  fi

  restart_post_api_services
}

cleanup_staging_directory
rebuild_env_from_example_if_needed
ensure_env_key GITHUB_OWNER "$GITHUB_OWNER"
echo "==> Persisting target image tag into .env before compose operations"
upsert_env_key GITHUB_OWNER "$GITHUB_OWNER"
upsert_env_key IMAGE_TAG "$IMAGE_TAG"
require_env_key PORTAL_IMAGE_TAG
print_disk_diagnostics
cleanup_if_root_usage_high "before image pull"

echo "==> Pulling image tag: $IMAGE_TAG"
if ! GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
  docker compose -f docker-compose.staging.yml pull $IMAGE_SERVICES; then
  echo "Initial image pull failed; retrying once after Docker cleanup"
  cleanup_docker_artifacts
  GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
    docker compose -f docker-compose.staging.yml pull $IMAGE_SERVICES
fi

echo "==> Stopping sync daemons before restart"
GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
  docker compose -f docker-compose.staging.yml stop datasync datasync-backfill || true

echo "==> Releasing backfill lock before restart"
if ! GITHUB_OWNER=$GITHUB_OWNER IMAGE_TAG=$IMAGE_TAG \
  docker compose -f docker-compose.staging.yml run --rm --no-deps api \
  python -c "from app.domains.extdata.dao.data_sync_status_dao import release_backfill_lock; release_backfill_lock()"
then
  echo "Backfill lock release skipped; continuing with restart"
fi

echo "==> Rolling restart"
if ! restart_services; then
  echo "Compose restart failed, cleaning staging backend services and retrying once"
  print_disk_diagnostics
  for name in \
    quantmate-api-1 \
    quantmate-worker-1 \
    quantmate-paper-runtime-1 \
    quantmate-datasync-1 \
    quantmate-datasync-backfill-1 \
    quantmate-rdagent-service-1; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
  docker compose -f docker-compose.staging.yml rm -sf $ALL_SERVICES >/dev/null 2>&1 || true
  cleanup_docker_artifacts
  restart_services
fi

echo "==> Waiting for API to remain healthy after full restart"
ok=0
for i in $(seq 1 24); do
  if curl -sf http://localhost:8000/health >/dev/null; then
    ok=1
    break
  fi
  echo "Health check retry $i/24..."
  sleep 5
done

if [ "$ok" -ne 1 ]; then
  echo "API did not become healthy in time, printing container status and logs"
  docker compose -f docker-compose.staging.yml ps
  docker compose -f docker-compose.staging.yml logs --tail=200 api
  exit 1
fi

restart_nginx

echo "==> Verifying proxied API health"
proxied_ok=0
for i in $(seq 1 12); do
  if curl -sf http://localhost/health >/dev/null; then
    proxied_ok=1
    break
  fi
  echo "Proxied health retry $i/12..."
  sleep 3
done

if [ "$proxied_ok" -ne 1 ]; then
  echo "Proxied API did not become healthy in time"
  docker compose -f docker-compose.staging.yml ps
  docker logs --tail=200 quantmate_nginx || true
  curl -i --max-time 10 http://localhost/health || true
  exit 1
fi

echo "==> Persisting deployed image tag into .env"
upsert_env_key GITHUB_OWNER "$GITHUB_OWNER"
upsert_env_key IMAGE_TAG "$IMAGE_TAG"

cleanup_managed_images
cleanup_if_root_usage_high "after managed image cleanup"

echo "Staging deploy successful: $IMAGE_TAG"