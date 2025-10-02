import os
import time
import requests
import pandas as pd
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import timezone


@dag(
    dag_id="dag_btc_daily",
    description="BTC-USD daily pipeline: extract -> load_raw -> metrics/plot",
    start_date=timezone.datetime(2024, 8, 21),
    schedule="@daily",
    catchup=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["crypto", "btc"]
)
def btc_daily_pipeline():
    """
    Daily Bitcoin price extraction pipeline.
    Downloads hourly BTCUSDT candles from Binance, saves to CSV and loads into SQLite.
    """

    @task()
    def extract():
        """
        Downloads hourly BTCUSDT candles from Binance for the logical date (UTC).
        Saves CSV to DATA_DIR/btc_prices_<YYYY-MM-DD>.csv with ts_utc and price (close).
        Returns {'day': YYYY-MM-DD, 'csv_path': ...}
        """
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        logical_date = ctx["logical_date"]
        day = logical_date.strftime("%Y-%m-%d")

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        os.makedirs(DATA_DIR, exist_ok=True)

        start = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.add(days=1)

        # Convert to milliseconds (Binance uses milliseconds epoch)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000) - 1

        out = os.path.join(DATA_DIR, f"btc_prices_{day}.csv")

        # Idempotency: if file exists and has content, don't call API again
        if os.path.exists(out) and os.path.getsize(out) > 0:
            return {"day": day, "csv_path": out}

        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000
        }

        last_err = None
        for attempt in range(4):
            try:
                r = requests.get(url, params=params, timeout=60)
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                
                data = r.json()
                if not isinstance(data, list) or not data:
                    raise RuntimeError("Empty or unexpected response from Binance")

                # Process candle data
                df = pd.DataFrame(data, columns=[
                    "open_time", "open", "high", "low", "close", "volume",
                    "close_time", "qav", "num_trades", "taker_base_vol",
                    "taker_quote_vol", "ignore"
                ])

                # Convert timestamp to UTC ISO format
                df["ts_utc"] = (
                    pd.to_datetime(df["open_time"], unit="ms", utc=True)
                    .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
                
                # Use closing price as representative price
                df["price"] = df["close"].astype(float)

                # Save processed data
                (df[["ts_utc", "price"]]
                    .drop_duplicates(subset=["ts_utc"])
                    .sort_values("ts_utc")
                    .to_csv(out, index=False))

                return {"day": day, "csv_path": out}
                
            except Exception as e:
                last_err = e
                time.sleep(2 * (attempt + 1))  # simple backoff: 2s, 4s, 6s, 8s

        # If all attempts failed, raise the last error
        raise last_err

    @task()
    def load_raw(meta: dict):
        """
        Reads CSV and loads into SQLite:
        - DB: DATA_DIR/crypto.db
        - Table: raw_prices(ts_utc TEXT, asset TEXT, price REAL)
        - Unique index (ts_utc, asset) to avoid duplicates
        Returns {'day': ...}
        """
        import sqlite3

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        os.makedirs(DATA_DIR, exist_ok=True)

        db_path = os.path.join(DATA_DIR, "crypto.db")
        csv_path = meta["csv_path"]
        day = meta["day"]

        # Load CSV
        df = pd.read_csv(csv_path)
        if df.empty:
            raise ValueError(f"Empty CSV: {csv_path}")

        # Add asset column and reorganize columns
        df["asset"] = "BTC-USD"
        df = df[["ts_utc", "asset", "price"]]

        # Connect to SQLite database
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_prices (
                ts_utc TEXT NOT NULL,
                asset  TEXT NOT NULL,
                price  REAL NOT NULL
            )
        """)
        
        # Create unique index to avoid duplicates
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_prices
            ON raw_prices (ts_utc, asset)
        """)

        # Insert data avoiding duplicates
        cur.executemany(
            "INSERT OR IGNORE INTO raw_prices (ts_utc, asset, price) VALUES (?, ?, ?)",
            list(df.itertuples(index=False, name=None))
        )
        
        con.commit()
        con.close()

        return {"day": day}

    @task()
    def compute_daily_metrics(meta: dict):
        """
        Calculates daily OHLC from raw_prices for 'day' (UTC) and does UPSERT into daily_metrics.
        """
        import sqlite3
        import pandas as pd
        from datetime import datetime, timezone as pytimezone

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        db_path = os.path.join(DATA_DIR, "crypto.db")

        day = meta["day"]
        day_start = f"{day}T00:00:00Z"
        day_end = f"{day}T23:59:59Z"

        con = sqlite3.connect(db_path)
        # Get intraday points for that day (ordered)
        df = pd.read_sql_query(
            """
            SELECT ts_utc, price
            FROM raw_prices
            WHERE asset='BTC-USD' AND ts_utc BETWEEN ? AND ?
            ORDER BY ts_utc ASC
            """,
            con,
            params=(day_start, day_end),
        )
        if df.empty:
            con.close()
            raise ValueError(f"No intraday data for {day} in raw_prices")

        # Daily OHLC (with our intraday points)
        o = float(df["price"].iloc[0])
        h = float(df["price"].max())
        l = float(df["price"].min())
        c = float(df["price"].iloc[-1])

        cur = con.cursor()
        # Create table if not exists
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_metrics (
              date  TEXT PRIMARY KEY,
              open  REAL,
              high  REAL,
              low   REAL,
              close REAL,
              ret   REAL,
              ma7   REAL,
              ma30  REAL,
              vol30 REAL
            )
            """
        )
        # UPSERT by date
        cur.execute(
            """
            INSERT INTO daily_metrics (date, open, high, low, close)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close
            """,
            (day, o, h, l, c),
        )
        con.commit()
        con.close()
        return {"day": day}

    @task()
    def enrich_indicators(meta: dict):
        """
        Calculates ret, ma7, ma30 and vol30 on daily_metrics and rewrites the table.
        ret = close.pct_change()
        ma7 = 7-day moving average of close
        ma30 = 30-day moving average of close
        vol30 = 30-day std of 'ret'
        """
        import os
        import sqlite3
        import pandas as pd

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        db_path = os.path.join(DATA_DIR, "crypto.db")

        con = sqlite3.connect(db_path)

        # Read entire history
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, ret, ma7, ma30, vol30 "
            "FROM daily_metrics ORDER BY date ASC",
            con
        )
        if df.empty:
            con.close()
            raise ValueError("daily_metrics is empty; run compute_daily_metrics first")

        # Recalculate indicators
        df["ret"] = df["close"].pct_change()
        df["ma7"] = df["close"].rolling(7, min_periods=7).mean()
        df["ma30"] = df["close"].rolling(30, min_periods=30).mean()
        df["vol30"] = df["ret"].rolling(30, min_periods=30).std()

        # Rewrite entire table (same form/PK)
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_metrics (
              date  TEXT PRIMARY KEY,
              open  REAL,
              high  REAL,
              low   REAL,
              close REAL,
              ret   REAL,
              ma7   REAL,
              ma30  REAL,
              vol30 REAL
            )
            """
        )
        # Replace content transactionally
        cur.execute("BEGIN")
        cur.execute("DELETE FROM daily_metrics")
        cur.executemany(
            "INSERT INTO daily_metrics (date, open, high, low, close, ret, ma7, ma30, vol30) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            list(df[["date","open","high","low","close","ret","ma7","ma30","vol30"]]
                 .itertuples(index=False, name=None))
        )
        con.commit()
        con.close()
        return {"day": meta["day"]}

    @task()
    def plot_report(meta: dict):
        """
        Reads the last 60 days from daily_metrics and generates a PNG with
        close, ma7 and ma30 in include/reports/btc_daily_<YYYY-MM-DD>.png
        """
        import os
        import sqlite3
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")  # non-interactive backend for containers
        import matplotlib.pyplot as plt

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        REPORTS_DIR = os.environ.get("REPORTS_DIR", "/opt/airflow/include/reports")
        os.makedirs(REPORTS_DIR, exist_ok=True)

        db_path = os.path.join(DATA_DIR, "crypto.db")
        day = meta["day"]
        out_path = os.path.join(REPORTS_DIR, f"btc_daily_{day}.png")

        con = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            """
            SELECT date, close, ma7, ma30
            FROM daily_metrics
            ORDER BY date ASC
            """,
            con
        )
        con.close()

        if df.empty:
            raise ValueError("daily_metrics is empty; run compute_daily_metrics first")

        # Take the last 60 CALENDAR days relative to the logical 'day'
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df = df.set_index("date")

        end_dt = pd.to_datetime(day).tz_localize("UTC")
        start_dt = end_dt - pd.Timedelta(days=60)
        df = df.loc[start_dt:end_dt]  # real 60-day window

        # If insufficient data, better to warn
        if df.empty:
            raise ValueError(f"No data in daily_metrics between {start_dt.date()} and {end_dt.date()}")

        # --- Plot
        import matplotlib.dates as mdates
        plt.figure(figsize=(13, 5))

        df["close"].plot(label="Close", linewidth=1.6)
        if df["ma7"].notna().any():
            df["ma7"].plot(label="MA7", linewidth=1.3)
        if df["ma30"].notna().any():
            df["ma30"].plot(label="MA30", linewidth=1.3)

        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=10))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))

        plt.title("BTC-USD – Last 60 days")
        plt.xlabel("Date")
        plt.ylabel("Price (USD)")
        plt.grid(True, alpha=0.25)
        plt.legend()
        plt.margins(x=0.01)     # no excessive margins
        plt.tight_layout()
        plt.savefig(out_path, dpi=140)
        plt.close()

        return {"day": day, "report_path": out_path}
    
    @task()
    def quality_checks(meta: dict):
        """
        Validates day artifacts and data:
        - CSV exists and not empty
        - raw_prices has >=20 rows for the day
        - daily_metrics has the day row with non-null OHLC
        - if >=30 days in daily_metrics, ma30 and vol30 not null
        - day PNG exists and has size >0
        """
        import os, sqlite3, pandas as pd

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        REPORTS_DIR = os.environ.get("REPORTS_DIR", "/opt/airflow/include/reports")

        day = meta["day"]
        csv_path = os.path.join(DATA_DIR, f"btc_prices_{day}.csv")
        png_path = os.path.join(REPORTS_DIR, f"btc_daily_{day}.png")
        db_path = os.path.join(DATA_DIR, "crypto.db")

        # 1) CSV
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            raise ValueError(f"[DQ] Missing or empty CSV: {csv_path}")

        # 2) raw_prices for the day (we expect ~24 candles/hour; accept >=20)
        con = sqlite3.connect(db_path)
        count = pd.read_sql_query(
            """
            SELECT COUNT(*) AS n
            FROM raw_prices
            WHERE asset='BTC-USD'
              AND ts_utc BETWEEN ? AND ?
            """,
            con,
            params=(f"{day}T00:00:00Z", f"{day}T23:59:59Z"),
        )["n"].iloc[0]
        if count < 20:
            con.close()
            raise ValueError(f"[DQ] Insufficient raw_prices for {day}: {count} rows (<20)")

        # 3) daily_metrics for the day with non-null OHLC
        dm = pd.read_sql_query(
            "SELECT * FROM daily_metrics WHERE date = ?",
            con, params=(day,)
        )
        if dm.empty:
            con.close()
            raise ValueError(f"[DQ] No row in daily_metrics for {day}")
        o, h, l, c = dm.loc[0, ["open","high","low","close"]]
        if any(pd.isna([o, h, l, c])):
            con.close()
            raise ValueError(f"[DQ] NULL OHLC in daily_metrics for {day}")

        # 4) If >=30 days in table, require ma30/vol30 not null
        total_days = pd.read_sql_query(
            "SELECT COUNT(*) AS n FROM daily_metrics", con
        )["n"].iloc[0]
        if total_days >= 30:
            ma30 = dm.loc[0, "ma30"]
            vol30 = dm.loc[0, "vol30"]
            if pd.isna(ma30) or pd.isna(vol30):
                con.close()
                raise ValueError(f"[DQ] NULL indicators with >=30d history for {day} (ma30={ma30}, vol30={vol30})")

        con.close()

        # 5) PNG
        if not os.path.exists(png_path) or os.path.getsize(png_path) == 0:
            raise ValueError(f"[DQ] Missing or empty PNG report: {png_path}")

        return {"day": day, "csv": csv_path, "png": png_path}


    meta = extract()
    d1 = load_raw(meta)
    d2 = compute_daily_metrics(d1)
    d3 = enrich_indicators(d2)
    d4 = plot_report(d3)
    quality_checks(d4)

# Create DAG instance
btc_daily_pipeline()