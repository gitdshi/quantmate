FROM python:3.11-slim

# Install Docker CLI for Docker-in-Docker execution (required by RD-Agent)
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    rm -rf /var/lib/apt/lists/*

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

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8001

CMD ["python", "app.py"]
