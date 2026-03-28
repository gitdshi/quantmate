# QuantMate API Dockerfile
FROM python:3.11

WORKDIR /app

ARG IMAGE_BUILD_TIME=unknown

# Install system dependencies including curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt ./

# Install Python dependencies using trusted HTTP mirror to bypass SSL issues
# Clear any proxy environment that might break apt in the build environment
ARG TARGETARCH
ENV http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= no_proxy= NO_PROXY=
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      grep -v '^pyqlib>=0\.9\.0$' requirements.txt > requirements.docker.txt; \
    else \
      cp requirements.txt requirements.docker.txt; \
    fi \
    && pip install --no-cache-dir \
    --index-url http://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn \
    -r requirements.docker.txt

# Copy application code
COPY app/ ./app/

# Copy migration scripts
COPY mysql/migrations/ ./mysql/migrations/

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
