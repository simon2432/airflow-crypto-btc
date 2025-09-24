import os, time, requests
import pandas as pd
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import timezone

@dag(
    dag_id="dag_btc_daily",
    description="BTC-USD diario: extract -> (luego load/metrics/plot)",
    start_date=timezone.datetime(2025, 9, 1),  # punto de inicio (pasado)
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["crypto", "btc"]
)
def btc_daily_pipeline():

    @task()
    def extract():
        """
        Descarga precios intradía de BTC/USD para la fecha lógica (UTC).
        Guarda CSV en DATA_DIR/btc_prices_<YYYY-MM-DD>.csv con ts_utc y price.
        Devuelve {'day': YYYY-MM-DD, 'csv_path': ...}
        """
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        logical_date = ctx["logical_date"]  # pendulum dt en UTC
        day = logical_date.strftime("%Y-%m-%d")

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        os.makedirs(DATA_DIR, exist_ok=True)

        start = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.add(days=1)  # pendulum add

        ts_from = int(start.timestamp())
        ts_to = int(end.timestamp())

        url = (
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
            f"?vs_currency=usd&from={ts_from}&to={ts_to}"
        )
        headers = {"User-Agent": "airflow-crypto-btc/1.0 (contact: your-email@example.com)"}

        last_err = None
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers, timeout=60)
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                data = r.json()
                prices = data.get("prices", [])
                if not prices:
                    raise RuntimeError("Respuesta sin datos (prices vacío)")

                # prices: [[ts_ms, price], ...]
                df = pd.DataFrame(prices, columns=["ts_ms", "price"])
                # a UTC ISO compacto (Z = Zulu/UTC)
                df["ts_utc"] = (
                    pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
                      .dt.tz_convert("UTC")
                      .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
                out = os.path.join(DATA_DIR, f"btc_prices_{day}.csv")
                (df[["ts_utc", "price"]]
                    .drop_duplicates(subset=["ts_utc"])
                    .sort_values("ts_utc")
                    .to_csv(out, index=False))
                return {"day": day, "csv_path": out}
            except Exception as e:
                last_err = e
                time.sleep(2 * (attempt + 1))  # backoff simple: 2s, 4s
        # Si falló 3 intentos, explota la task (Airflow hará retries según default_args)
        raise last_err

    # Por ahora, sólo ejecutamos extract para probarla
    extract()

btc_daily_pipeline()
