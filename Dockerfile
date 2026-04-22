# syntax=docker/dockerfile:1.7

# QuantMate API Dockerfile
FROM python:3.11

WORKDIR /app

ARG IMAGE_BUILD_TIME=unknown
ARG PIP_INDEX_URL=http://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

# Install system dependencies including curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests first for layer caching
COPY requirements.txt requirements.runtime.txt ./

# Install Python dependencies using trusted HTTP mirror to bypass SSL issues
# Clear any proxy environment that might break apt in the build environment
ARG TARGETARCH
ENV http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= no_proxy= NO_PROXY=
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ "$TARGETARCH" = "arm64" ]; then \
      grep -v '^pyqlib>=0\.9\.0$' requirements.runtime.txt > requirements.docker.txt; \
    else \
      cp requirements.runtime.txt requirements.docker.txt; \
    fi \
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
    fi

# Copy application code
COPY app/ ./app/

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

# Expose port
EXPOSE 8000

# Run migrations then start the API server
CMD ["sh", "-c", "export APP_BUILD_TIME=\"${APP_BUILD_TIME:-$(cat /opt/quantmate-build/build_time 2>/dev/null || echo unknown)}\"; python -m app.infrastructure.db.migrate && uvicorn app.api.main:app --host 0.0.0.0 --port 8000"]
