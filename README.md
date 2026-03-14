# Airflow Crypto BTC

Educational pipeline built with Apache Airflow to orchestrate the daily extraction of Bitcoin (BTC-USD) prices, compute key metrics, and generate reports. This project demonstrates how to design, containerize, and run an end-to-end data pipeline using modern data engineering practices.

## Features

- **Dockerized Airflow stack** with Postgres backend (webserver, scheduler, triggerer)
- **Daily DAG** (`dag_btc_daily`) that runs every day with catchup enabled (processes all days from start_date to today on first run)
- **Data extraction** from Binance API for BTC-USDT hourly candles
- **Rate limit handling** (HTTP 429) with Retry-After header support
- **Data storage** in SQLite database with proper indexing and deduplication
- **Financial metrics computation** including OHLC, moving averages, and volatility
- **Quality checks** with comprehensive data validation
- **Report generation** with matplotlib charts showing price trends
- **Retry logic** with exponential backoff for API failures

## Pipeline Overview

The `dag_btc_daily` DAG executes the following tasks in sequence:

1. **Extract**: Downloads hourly BTC-USDT candles from Binance API for the logical date
2. **Load Raw**: Stores raw price data in SQLite (`raw_prices` table)
3. **Compute Daily Metrics**: Calculates OHLC values from intraday data (`daily_metrics` table)
4. **Enrich Indicators**: Computes financial indicators (returns, moving averages, volatility)
5. **Plot Report**: Generates PNG chart with price trends and moving averages
6. **Quality Checks**: Validates data integrity and file existence

### Financial Indicators

- **ret**: Daily percentage return (price change)
- **ma7**: 7-day moving average of closing prices
- **ma30**: 30-day moving average of closing prices
- **vol30**: 30-day rolling volatility (standard deviation of returns)

### Database Schema

**raw_prices** table:

- `ts_utc`: Timestamp in UTC (TEXT)
- `asset`: Asset symbol (TEXT, default: 'BTC-USD')
- `price`: Closing price (REAL)
- Unique index on (ts_utc, asset)

**daily_metrics** table:

- `date`: Date in YYYY-MM-DD format (PRIMARY KEY)
- `open`, `high`, `low`, `close`: OHLC values (REAL)
- `ret`, `ma7`, `ma30`, `vol30`: Financial indicators (REAL)

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Git

### Setup

```bash
# Clone repository
git clone https://github.com/simon2432/airflow-crypto-btc.git
cd airflow-crypto-btc

# Build and start the Airflow stack (run from docker/ folder)
cd docker
docker compose up -d
```

On first run, `airflow-init` will initialize the database and create the admin user (airflow/airflow). This may take 1–2 minutes before the webserver becomes available.

### Access Airflow UI

- **Web UI**: http://localhost:8080
- **Default credentials**: airflow / airflow (can be overridden in .env)

## Project Structure

```
airflow-crypto-btc/
├── dags/
│   └── dag_btc_daily.py     # Main BTC pipeline DAG
├── docker/
│   ├── Dockerfile           # Airflow image with dependencies
│   ├── docker-compose.yml   # Postgres + Airflow (run: cd docker && docker compose up -d)
│   └── pgdata/              # Postgres data (created on first run, gitignored)
├── data/                    # SQLite DB and daily CSVs (gitignored)
├── include/
│   └── reports/             # Generated PNG charts (gitignored)
├── logs/                    # Task execution logs (gitignored)
├── .env.example             # Configuration template
├── requirements.txt         # Python dependencies
└── README.md
```

## Configuration

The pipeline uses environment variables for configuration. Copy `.env.example` to `docker/.env` (same folder as docker-compose.yml) and adjust:

| Variable      | Description                    | Default                        |
| ------------- | ------------------------------ | ------------------------------ |
| `DATA_DIR`    | Directory for DB and CSV files | `/opt/airflow/data`            |
| `REPORTS_DIR` | Directory for PNG reports      | `/opt/airflow/include/reports` |

## Output Files

- **Daily CSVs**: `data/btc_prices_YYYY-MM-DD.csv` with hourly price data
- **Database**: `data/crypto.db` SQLite database with all historical data
- **Reports**: `include/reports/btc_daily_YYYY-MM-DD.png` with 60-day price charts
- **Logs**: Detailed execution logs in the Airflow logs directory

## Data Quality

The pipeline includes comprehensive quality checks:

- Validates CSV file existence and content
- Ensures minimum 15 hourly data points per day
- Verifies OHLC values are not null
- Checks moving averages and volatility for datasets ≥31 days
- Confirms PNG report generation and file size
