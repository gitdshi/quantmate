#!/bin/bash

# TraderMate RQ Worker Startup Script
# This script starts RQ workers for background job processing

set -e

# Change to project root
cd "$(dirname "$0")/.."

echo "🔧 Starting TraderMate RQ Workers..."

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "📦 Activating virtual environment..."
    source .venv/bin/activate
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

echo "🔧 Configuration:"
echo "   MySQL: ${MYSQL_HOST}:${MYSQL_PORT}"
echo "   Redis: ${REDIS_HOST}:${REDIS_PORT}"

# Start workers
echo "👷 Starting workers..."
echo "   - backtest queue worker"
echo "   - optimization queue worker"

# Start workers in background
rq worker --url "redis://${REDIS_HOST}:${REDIS_PORT}/0" backtest optimization default high low &

echo "✅ Workers started successfully"
echo "📊 Monitor workers: rq info --url redis://${REDIS_HOST}:${REDIS_PORT}/0"

# Wait for workers
wait
