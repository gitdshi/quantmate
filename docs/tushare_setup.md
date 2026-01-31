Tushare ingestion setup and quick run

Overview
- This guide shows how to start a MySQL instance (via docker-compose), apply the initialization SQL (`mysql/init/init.sql`), install Python deps, and run a sample Tushare ingestion for `daily` data.

Preconditions
- Docker & docker-compose installed and running on your machine.
- A Tushare Pro token (set as `TUSHARE_TOKEN`).

1) Start services (MySQL + app image)

```bash
cd /path/to/tradermate
docker-compose up -d mysql
# wait ~20s for MySQL to initialize; the init scripts in ./mysql/init are mounted into the container and will be executed automatically
```

2) Verify MySQL is running and init SQL was applied

```bash
# connect to mysql container
docker exec -it tradermate_mysql mysql -uroot -ppassword -e "SHOW DATABASES;"
# confirm `tradermate` exists
# optionally list tables
docker exec -it tradermate_mysql mysql -uroot -ppassword -D tradermate -e "SHOW TABLES;"
```

3) Create a local `.env` (or export env vars) — see `.env.example`

4) Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

5) Run a sample ingestion (daily) — set your Tushare token and DB URL first

```bash
export TUSHARE_TOKEN="your_token_here"
export DATABASE_URL="mysql+pymysql://root:password@127.0.0.1:3306/tradermate?charset=utf8mb4"
python3 app/services/tushare_ingest.py
```

Notes
- The project already mounts `mysql/init/init.sql` into the MySQL container; starting the `mysql` service will run the init script and create all tables.
- If you prefer to run SQL manually, you can execute:

```bash
docker cp mysql/init/init.sql tradermate_mysql:/init.sql
docker exec -it tradermate_mysql bash -c "mysql -uroot -ppassword tradermate < /init.sql"
```

- The ingestion script `app/services/tushare_ingest.py` expects `TUSHARE_TOKEN` and `DATABASE_URL` environment variables. It contains functions for `daily`, `daily_basic`, `adj_factor`, and `income` ingestion and writes run metadata into `tushare_stock_ingest_audit`.

Troubleshooting
- If the init SQL fails due to syntax or engine/version issues, check MySQL logs:

```bash
docker logs tradermate_mysql
```

- If `tushare` fails to authenticate, verify your token and network access.

Next steps
- Expand the ingestor to cover more endpoints and add bulk-insert optimization.
- Add unit tests for ingest functions and mapping validations.
