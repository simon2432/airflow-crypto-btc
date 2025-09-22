# Airflow Crypto BTC

Pipeline educativo para orquestar con Apache Airflow la extracción diaria de precios de Bitcoin (BTC-USD), cálculo de métricas y generación de reportes. Proyecto paso a paso (commits incrementales).

## Roadmap (high level)
- Fase A: Repo base (este commit).
- Fase B: Docker + Airflow stack.
- Fase C: DAG de “hola mundo”.
- Fase D: DAG BTC diario (extract → load → métricas → gráficos).
- Fase E: Backfill + calidad de datos.
- Fase F: Documentación y mejoras.

## Estado actual
- [x] Estructura base del repo
- [x] requirements.txt con dependencias de DAGs
- [x] Dockerfile personalizado basado en apache/airflow
- [x] docker-compose con Postgres + Airflow (webserver/scheduler/triggerer)

## Próximo paso
- Fase C: Build de la imagen e inicialización de Airflow (db init, usuario admin) y levantar la UI.
