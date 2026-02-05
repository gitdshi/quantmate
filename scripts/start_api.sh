#!/bin/bash

# TraderMate API Startup Script
# This script starts the FastAPI server in development mode

set -e

# Change to project root
cd "$(dirname "$0")/.."

echo "🚀 Starting TraderMate API..."

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Install dependencies if needed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "📥 Installing API dependencies..."
    pip install -r requirements-api.txt
fi

# Set environment variables for local development
export MYSQL_HOST=${MYSQL_HOST:-localhost}
export MYSQL_PORT=${MYSQL_PORT:-3306}
export MYSQL_USER=${MYSQL_USER:-root}
export MYSQL_PASSWORD=${MYSQL_PASSWORD:-password}
export TUSHARE_DATABASE=${TUSHARE_DATABASE:-tushare}
export VNPY_DATABASE=${VNPY_DATABASE:-vnpy}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export JWT_SECRET_KEY=${JWT_SECRET_KEY:-dev-secret-key-change-me}
export DEBUG=${DEBUG:-true}

echo "🔧 Configuration:"
echo "   MySQL: ${MYSQL_HOST}:${MYSQL_PORT}"
echo "   Redis: ${REDIS_HOST}:${REDIS_PORT}"
echo "   Debug: ${DEBUG}"

# Start the server
echo "🌐 Starting server at http://0.0.0.0:8000"
echo "📖 API docs available at http://localhost:8000/docs"

python -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
