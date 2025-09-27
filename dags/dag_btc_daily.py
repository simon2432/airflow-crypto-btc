import os
import time
import requests
import pandas as pd
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import timezone


@dag(
    dag_id="dag_btc_daily",
    description="BTC-USD diario: extract -> load_raw -> (luego metrics/plot)",
    start_date=timezone.datetime(2025, 8, 21),  # punto de inicio (pasado)
    schedule="@daily",
    catchup=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["crypto", "btc"]
)
def btc_daily_pipeline():
    """
    Pipeline diario para extraer datos de precios de Bitcoin.
    Descarga velas horarias de BTCUSDT desde Binance, las guarda en CSV
    y las carga en base de datos SQLite.
    """

    @task()
    def extract():
        """
        Descarga velas horarias de BTCUSDT (Binance) para la fecha lógica (UTC).
        Guarda CSV en DATA_DIR/btc_prices_<YYYY-MM-DD>.csv con ts_utc y price (close).
        Devuelve {'day': YYYY-MM-DD, 'csv_path': ...}
        """
        from airflow.operators.python import get_current_context

        # Obtener contexto de Airflow
        ctx = get_current_context()
        logical_date = ctx["logical_date"]  # pendulum dt en UTC
        day = logical_date.strftime("%Y-%m-%d")

        # Configurar directorio de datos
        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        os.makedirs(DATA_DIR, exist_ok=True)

        # Definir rango de tiempo para el día (UTC)
        start = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.add(days=1)

        # Convertir a milisegundos (Binance usa milisegundos epoch)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000) - 1  # inclusivo

        # Ruta del archivo de salida
        out = os.path.join(DATA_DIR, f"btc_prices_{day}.csv")

        # Idempotencia: si ya existe y tiene contenido, no llamamos a la API de nuevo
        if os.path.exists(out) and os.path.getsize(out) > 0:
            return {"day": day, "csv_path": out}

        # Configurar parámetros para la API de Binance
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": "BTCUSDT",     # USDT ~ USD
            "interval": "1h",        # velas de 1 hora (24 por día)
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000            # más que suficiente para 1 día
        }

        # Intentar descarga con reintentos
        last_err = None
        for attempt in range(4):
            try:
                # Hacer petición a la API
                r = requests.get(url, params=params, timeout=60)
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                
                data = r.json()
                if not isinstance(data, list) or not data:
                    raise RuntimeError("Respuesta vacía o inesperada de Binance")

                # Procesar datos de velas
                # Estructura kline: [openTime, open, high, low, close, volume, closeTime, ...]
                df = pd.DataFrame(data, columns=[
                    "open_time", "open", "high", "low", "close", "volume",
                    "close_time", "qav", "num_trades", "taker_base_vol",
                    "taker_quote_vol", "ignore"
                ])

                # Convertir timestamp a formato UTC ISO
                df["ts_utc"] = (
                    pd.to_datetime(df["open_time"], unit="ms", utc=True)
                    .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
                
                # Usar precio de cierre como precio representativo
                df["price"] = df["close"].astype(float)

                # Guardar datos procesados
                (df[["ts_utc", "price"]]
                    .drop_duplicates(subset=["ts_utc"])
                    .sort_values("ts_utc")
                    .to_csv(out, index=False))

                return {"day": day, "csv_path": out}
                
            except Exception as e:
                last_err = e
                time.sleep(2 * (attempt + 1))  # backoff simple: 2s, 4s, 6s, 8s

        # Si todos los intentos fallaron, lanzar el último error
        raise last_err

    @task()
    def load_raw(meta: dict):
        """
        Lee el CSV y lo carga en SQLite:
        - DB: DATA_DIR/crypto.db
        - Tabla: raw_prices(ts_utc TEXT, asset TEXT, price REAL)
        - Índice único (ts_utc, asset) para evitar duplicados
        Devuelve {'day': ...}
        """
        import sqlite3

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        os.makedirs(DATA_DIR, exist_ok=True)

        db_path = os.path.join(DATA_DIR, "crypto.db")
        csv_path = meta["csv_path"]
        day = meta["day"]

        # Cargar CSV
        df = pd.read_csv(csv_path)  # ts_utc, price
        if df.empty:
            raise ValueError(f"CSV vacío: {csv_path}")

        # Añadir columna de asset y reorganizar columnas
        df["asset"] = "BTC-USD"
        df = df[["ts_utc", "asset", "price"]]

        # Conectar a base de datos SQLite
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        
        # Crear tabla si no existe
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_prices (
                ts_utc TEXT NOT NULL,
                asset  TEXT NOT NULL,
                price  REAL NOT NULL
            )
        """)
        
        # Crear índice único para evitar duplicados
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_prices
            ON raw_prices (ts_utc, asset)
        """)

        # Insertar datos evitando duplicados
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
        Calcula OHLC diario a partir de raw_prices para 'day' (UTC) y hace UPSERT en daily_metrics.
        """
        import sqlite3
        import pandas as pd
        from datetime import datetime, timezone as pytimezone

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        db_path = os.path.join(DATA_DIR, "crypto.db")

        day = meta["day"]  # 'YYYY-MM-DD'
        day_start = f"{day}T00:00:00Z"
        day_end   = f"{day}T23:59:59Z"

        con = sqlite3.connect(db_path)
        # Traemos los puntos intradía de ese día (ordenados)
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
            raise ValueError(f"Sin datos intradía para {day} en raw_prices")

        # OHLC diario (con nuestros puntos intradía)
        o = float(df["price"].iloc[0])
        h = float(df["price"].max())
        l = float(df["price"].min())
        c = float(df["price"].iloc[-1])

        cur = con.cursor()
        # Crear tabla si no existe
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
        # UPSERT por date
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
        Calcula ret, ma7, ma30 y vol30 sobre daily_metrics y reescribe la tabla.
        ret = close.pct_change()
        ma7 = media móvil 7 días de close
        ma30 = media móvil 30 días de close
        vol30 = std 30 días de 'ret'
        """
        import os
        import sqlite3
        import pandas as pd

        DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
        db_path = os.path.join(DATA_DIR, "crypto.db")

        con = sqlite3.connect(db_path)

        # Leer todo el histórico
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, ret, ma7, ma30, vol30 "
            "FROM daily_metrics ORDER BY date ASC",
            con
        )
        if df.empty:
            con.close()
            raise ValueError("daily_metrics está vacío; correr compute_daily_metrics primero")

        # Recalcular indicadores
        df["ret"] = df["close"].pct_change()
        df["ma7"] = df["close"].rolling(7, min_periods=7).mean()
        df["ma30"] = df["close"].rolling(30, min_periods=30).mean()
        df["vol30"] = df["ret"].rolling(30, min_periods=30).std()

        # Reescribir la tabla completa (misma forma/PK)
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
        # Reemplazar contenido de forma transaccional
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
    # Definir el flujo del pipeline
    # extract() -> load_raw() -> (futuras tasks: metrics, plot)
    meta = extract()
    d1 = load_raw(meta)
    d2 = compute_daily_metrics(d1)
    enrich_indicators(d2)

# Crear instancia del DAG
btc_daily_pipeline()