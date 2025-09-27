Airflow Crypto BTC

Educational pipeline built with Apache Airflow to orchestrate the daily extraction of Bitcoin (BTC-USD) prices, compute key metrics, and generate reports.
This project demonstrates how to design, containerize, and run an end-to-end data pipeline using modern data engineering practices.

📌 Features

Dockerized Airflow stack with Postgres backend (webserver, scheduler, triggerer).

Daily DAG (dag_btc_daily) that:

Extracts intraday BTC prices from the Binance API.

Loads data into a SQLite/Postgres database.

Computes OHLC (open, high, low, close) values per day.

Derives financial indicators:

ret: daily log return.

ma7, ma30: 7- and 30-day moving averages.

vol30: 30-day rolling volatility.

Clean project structure with incremental commits for learning.

🚀 Getting Started
Prerequisites

Docker & Docker Compose

Git

Setup
# Clone repository
git clone https://github.com/<your-user>/airflow-crypto-btc.git
cd airflow-crypto-btc

# Build and start the Airflow stack
docker compose up -d

Access Airflow UI

Web UI: http://localhost:8080

Default credentials: airflow / airflow (can be overridden in .env).

📂 Project Structure
airflow-crypto-btc/
│── dags/               # Airflow DAGs
│   └── dag_btc_daily.py
│── data/               # Output CSVs and SQLite DB
│── docker/             # Docker configuration (Dockerfile, compose, etc.)
│── logs/               # Task execution logs
│── requirements.txt    # Python dependencies