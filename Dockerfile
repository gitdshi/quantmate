# syntax=docker/dockerfile:1.7

# QuantMate API Dockerfile
FROM python:3.11

WORKDIR /app

ARG IMAGE_BUILD_TIME=unknown
ARG PIP_INDEX_URL=http://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

# Install system dependencies including curl for healthcheck and Docker CLI for RD-Agent runs
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  docker.io \
    && rm -rf /var/lib/apt/lists/*

RUN cat <<'EOF' >/usr/local/bin/conda
#!/bin/sh
if [ "$1" = "run" ]; then
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      -n|--name|--prefix)
        shift 2
        ;;
      --no-capture-output)
        shift
        ;;
      *)
        break
        ;;
    esac
  done

  if [ "$1" = "env" ]; then
    env PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"
    exit 0
  fi

  PATH="/usr/local/bin:/usr/bin:/bin:${PATH}" exec "$@"
fi

echo "Unsupported conda invocation: $*" >&2
exit 1
EOF
RUN chmod +x /usr/local/bin/conda

# Copy dependency manifest first for layer caching
COPY requirements.txt ./

# Install Python dependencies using trusted HTTP mirror to bypass SSL issues
# Clear any proxy environment that might break apt in the build environment
ARG TARGETARCH
ENV http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= no_proxy= NO_PROXY=
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ "$TARGETARCH" = "arm64" ]; then \
      grep -v '^pyqlib>=0\.9\.0$' requirements.txt > requirements.docker.txt; \
    else \
      cp requirements.txt requirements.docker.txt; \
    fi \
    && grep -Ev '^(pyqtgraph|PySide6|PySide6_Addons|PySide6_Essentials|QDarkStyle|QtPy|shiboken6|vnpy|vnpy_ctabacktester|vnpy_ctastrategy|vnpy_portfoliostrategy|vnpy_datamanager|vnpy_mysql|vnpy_sqlite|vnpy_tushare|rdagent|azureml-mlflow|mlflow|mlflow-skinny)==' requirements.docker.txt > requirements.docker.filtered.txt \
    && mv requirements.docker.filtered.txt requirements.docker.txt \
    && if [ -n "$PIP_INDEX_URL" ] && [ -n "$PIP_TRUSTED_HOST" ]; then \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        --index-url "$PIP_INDEX_URL" \
        --trusted-host "$PIP_TRUSTED_HOST" \
        -r requirements.docker.txt; \
    elif [ -n "$PIP_INDEX_URL" ]; then \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        --index-url "$PIP_INDEX_URL" \
        -r requirements.docker.txt; \
    else \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        -r requirements.docker.txt; \
    fi \
    && if [ -n "$PIP_INDEX_URL" ] && [ -n "$PIP_TRUSTED_HOST" ]; then \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        --no-deps \
        --index-url "$PIP_INDEX_URL" \
        --trusted-host "$PIP_TRUSTED_HOST" \
        vnpy==4.3.0 \
        vnpy_ctabacktester==1.3.0 \
        vnpy_ctastrategy==1.4.0 \
        vnpy_portfoliostrategy==1.2.2 \
        vnpy_datamanager==1.2.0 \
        vnpy_mysql==1.1.1 \
        vnpy_sqlite==1.1.3 \
        vnpy_tushare==1.4.21.0; \
    elif [ -n "$PIP_INDEX_URL" ]; then \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        --no-deps \
        --index-url "$PIP_INDEX_URL" \
        vnpy==4.3.0 \
        vnpy_ctabacktester==1.3.0 \
        vnpy_ctastrategy==1.4.0 \
        vnpy_portfoliostrategy==1.2.2 \
        vnpy_datamanager==1.2.0 \
        vnpy_mysql==1.1.1 \
        vnpy_sqlite==1.1.3 \
        vnpy_tushare==1.4.21.0; \
    else \
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install \
        --no-deps \
        vnpy==4.3.0 \
        vnpy_ctabacktester==1.3.0 \
        vnpy_ctastrategy==1.4.0 \
        vnpy_portfoliostrategy==1.2.2 \
        vnpy_datamanager==1.2.0 \
        vnpy_mysql==1.1.1 \
        vnpy_sqlite==1.1.3 \
        vnpy_tushare==1.4.21.0; \
    fi

# Copy application code
COPY app/ ./app/
COPY strategies/ ./strategies/

# Copy scripts used by staging SOP/DataSync bootstrap
COPY scripts/ ./scripts/

# Copy SQL artifacts used by runtime bootstrap and migrations
COPY mysql/migrations/ ./mysql/migrations/
COPY mysql/init/ ./mysql/init/

# Keep the project root importable even when docker exec runs outside /app.
ENV PYTHONPATH=/app

# Persist image build metadata for runtime env injection
RUN mkdir -p /opt/quantmate-build \
    && if [ "$IMAGE_BUILD_TIME" = "unknown" ]; then \
      date -u +"%Y-%m-%dT%H:%M:%SZ" > /opt/quantmate-build/build_time; \
    else \
      echo "$IMAGE_BUILD_TIME" > /opt/quantmate-build/build_time; \
    fi

# Expose ports
EXPOSE 8000
EXPOSE 8001

# Run migrations then start the API server
CMD ["sh", "-c", "export APP_BUILD_TIME=\"${APP_BUILD_TIME:-$(cat /opt/quantmate-build/build_time 2>/dev/null || echo unknown)}\"; python -m app.infrastructure.db.migrate && uvicorn app.api.main:app --host 0.0.0.0 --port 8000"]
