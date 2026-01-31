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

   # Install Python dependencies
   pip install -r requirements.txt

   # Start the main application
   python app/main.py
   ```

4. **Start Data Synchronization** (Optional):
   (Data synchronization service removed.)

## Components

### Core Application (`app/main.py`)
- vnpy GUI application
- CTA strategy development and backtesting
- Data management interface

### Data Synchronization
The previous data synchronization daemon and startup scripts have been removed from this repository. Use `app/services/tushare_ingest.py` and `scripts/fetch_tushare_accessible.py` for controlled ingestion.

### Database
- MySQL 8.0 with persistent volumes
- Pre-configured schema for trading data
- Docker containerized for easy deployment

## Configuration

### Environment Variables (`.env`)
```bash
# Database
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=tradermate

# Tushare API
TUSHARE_TOKEN=your_token_here

# Data Sync
SYNC_INTERVAL_HOURS=24
BATCH_SIZE=100
LOG_LEVEL=INFO
```

## Data Synchronization
The external data synchronization daemon and related documentation were removed. Refer to `docs/tushare_setup.md` and `app/services/tushare_ingest.py` for data ingestion and setup instructions.

## Docker Services

### MySQL Database
- **Port**: 3306 (exposed to host)
- **Database**: tradermate
- **Persistent Data**: Stored in Docker volume
- **Initialization**: Automatic schema creation

### Application Container
- Python 3.11 environment
- All dependencies pre-installed
- Connected to MySQL network

## Development

### Project Structure
```
tradermate/
├── app/
│   ├── main.py              # Main vnpy application
│   ├── data.py              # Data fetching utilities
│   └── services/
│       └── (data sync service removed)
├── mysql/
│   └── init/
│       └── init.sql         # Database initialization
├── logs/                    # Application logs
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Docker services
├── Dockerfile              # Application container
└── .env.example           # Environment template
```

### Adding New Features
1. Follow the existing code structure
2. Add proper logging and error handling
3. Update requirements.txt for new dependencies
4. Test with Docker Compose environment
5. Update documentation

## Troubleshooting

### Common Issues
1. **MySQL Connection Failed**: Ensure Docker containers are running
2. **Tushare API Errors**: Verify token and account status
3. **Import Errors**: Check Python environment and dependencies
4. **Permission Issues**: Verify file permissions for logs and data directories

### Logs
- Application logs: `logs/` (data-sync specific logs removed)
- Docker logs: `docker-compose logs`
- MySQL logs: `docker-compose logs mysql`

## License

[Add your license information here]

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
- Review Docker Compose logs
- Verify environment configuration