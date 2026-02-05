# TraderMate

A comprehensive trading platform built with vnpy framework, featuring automated data synchronization from Tushare and MySQL database integration.

## Features

- **vnpy Trading Framework**: Full-featured trading platform with CTA strategies and backtesting
- **Automated Data Sync**: Continuous synchronization of stock data from Tushare API
- **MySQL Database**: Persistent storage with Docker containerization
- **Docker Integration**: Complete containerized environment
- **Data Management**: Built-in data manager for viewing and managing trading data

## Quick Start

1. **Clone and Setup**:
   ```bash
   git clone <repository-url>
   cd tradermate
   ```

2. **Environment Setup**:
   ```bash
   # Copy environment template
   cp .env.example .env

   # Edit .env with your Tushare token and other configurations
   nano .env
   ```

3. **Start Services**:
   ```bash
   # Start MySQL database
   docker-compose up -d mysql

   # TraderMate

   This repository's full documentation has been moved to the `docs/` folder. Open `docs/README.md` for the consolidated project documentation (overview, architecture, API reference, deployment and testing instructions).

   See `docs/README.md`.