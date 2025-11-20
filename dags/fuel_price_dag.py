"""
DAG: Fuel Price ETL Pipeline
Ejecuta el pipeline ETL una vez al día a las 2 AM
Carga datos a PostgreSQL (staging local) y Redshift (producción) en paralelo
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)


def run_extract():
    """Ejecuta el paso de extracción del ETL."""
    try:
        from fuel_price.extract import extract_all_data, get_default_data_path
        from fuel_price.config import START_DATE_BRENT

        logger.info("Iniciando extracción de datos...")

        update_all = Variable.get(
            "fuel_etl_update_all", default_var=True, deserialize_json=True
        )
        brent_start = Variable.get(
            "fuel_etl_brent_start_date", default_var=START_DATE_BRENT
        )

        brent, fuel, dolar = extract_all_data(
            brent_start_date=brent_start,
            fuel_data_path=get_default_data_path(),
            update_all=update_all,
        )

        logger.info(f"Extracción completada exitosamente")
        logger.info(f"  - Brent: {len(brent):,} registros")
        logger.info(f"  - Fuel: {len(fuel):,} registros")
        logger.info(f"  - Dolar: {len(dolar):,} registros")

    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}", exc_info=True)
        raise


def run_transform():
    """Ejecuta el paso de transformación del ETL."""
    try:
        from fuel_price.transform import (
            process_brent_price_data,
            process_fuel_data_pipeline,
            process_dolar_price_data,
        )
        from fuel_price.extract import get_project_root
        import pandas as pd

        logger.info("Iniciando transformación de datos...")

        project_root = get_project_root()
        raw_path = project_root / "data" / "raw"

        logger.info(f"Leyendo datos desde: {raw_path}")

        brent_raw = pd.read_csv(raw_path / "brent_prices.csv")
        fuel_raw = pd.read_csv(raw_path / "precios_eess_completo.csv")
        dolar_raw = pd.read_csv(raw_path / "usd_ars_bluelytics.csv")

        logger.info("Datos cargados, iniciando procesamiento...")

        process_brent_price_data(brent_raw)
        process_fuel_data_pipeline(fuel_raw)
        process_dolar_price_data(dolar_raw)

        logger.info("Transformación completada exitosamente")

    except FileNotFoundError as e:
        logger.error(f"Archivo no encontrado: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        raise


def run_load_postgres():
    """Carga datos a PostgreSQL (staging local)."""
    try:
        from fuel_price.load import load_all_data
        from fuel_price.extract import get_project_root
        import pandas as pd

        logger.info("Iniciando carga de datos a PostgreSQL...")

        project_root = get_project_root()
        processed_path = project_root / "data" / "processed"

        logger.info(f"Leyendo datos procesados desde: {processed_path}")

        brent_clean = pd.read_parquet(processed_path / "brent_price_cleaned.parquet")
        fuel_clean = pd.read_parquet(processed_path / "fuel_price_cleaned.parquet")
        usd_ars_clean = pd.read_parquet(processed_path / "dollar_price_cleaned.parquet")

        brent_analytics = pd.read_parquet(
            processed_path / "brent_price_monthly.parquet"
        )
        fuel_analytics = pd.read_parquet(
            processed_path / "fuel_price_aggregated.parquet"
        )
        usd_ars_analytics = pd.read_parquet(
            processed_path / "dollar_price_aggregated.parquet"
        )

        logger.info("Archivos parquet cargados exitosamente")
        logger.info(f"  - Brent clean: {len(brent_clean):,} registros")
        logger.info(f"  - Fuel clean: {len(fuel_clean):,} registros")
        logger.info(f"  - USD/ARS clean: {len(usd_ars_clean):,} registros")

        load_all_data(
            brent_clean=brent_clean,
            fuel_clean=fuel_clean,
            usd_ars_clean=usd_ars_clean,
            brent_analytics=brent_analytics,
            fuel_analytics=fuel_analytics,
            usd_ars_analytics=usd_ars_analytics,
        )

        logger.info("Carga a PostgreSQL completada exitosamente")

    except FileNotFoundError as e:
        logger.error(f"Archivo parquet no encontrado: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error en carga a PostgreSQL: {str(e)}", exc_info=True)
        raise


def run_load_redshift():
    """Carga datos a AWS Redshift (producción)."""
    try:
        from fuel_price.load_redshift import load_all_data_to_redshift
        from fuel_price.extract import get_project_root
        import pandas as pd

        logger.info("Iniciando carga de datos a Redshift...")

        project_root = get_project_root()
        processed_path = project_root / "data" / "processed"

        logger.info(f"Leyendo datos procesados desde: {processed_path}")

        brent_clean = pd.read_parquet(processed_path / "brent_price_cleaned.parquet")
        fuel_clean = pd.read_parquet(processed_path / "fuel_price_cleaned.parquet")
        usd_ars_clean = pd.read_parquet(processed_path / "dollar_price_cleaned.parquet")

        brent_analytics = pd.read_parquet(
            processed_path / "brent_price_monthly.parquet"
        )
        fuel_analytics = pd.read_parquet(
            processed_path / "fuel_price_aggregated.parquet"
        )
        usd_ars_analytics = pd.read_parquet(
            processed_path / "dollar_price_aggregated.parquet"
        )

        logger.info("Archivos parquet cargados exitosamente")
        logger.info(f"  - Brent clean: {len(brent_clean):,} registros")
        logger.info(f"  - Fuel clean: {len(fuel_clean):,} registros")
        logger.info(f"  - USD/ARS clean: {len(usd_ars_clean):,} registros")

        load_all_data_to_redshift(
            brent_clean=brent_clean,
            fuel_clean=fuel_clean,
            usd_ars_clean=usd_ars_clean,
            brent_analytics=brent_analytics,
            fuel_analytics=fuel_analytics,
            usd_ars_analytics=usd_ars_analytics,
        )

        logger.info("Carga a Redshift completada exitosamente")

    except FileNotFoundError as e:
        logger.error(f"Archivo parquet no encontrado: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error en carga a Redshift: {str(e)}", exc_info=True)
        raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fuel_price_etl",
    default_args=default_args,
    description="Pipeline ETL de precios de combustibles con carga paralela a PostgreSQL y Redshift",
    schedule_interval="0 2 * * *",  # Todos los días a las 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "fuel_prices", "postgres", "redshift"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_postgres_task = PythonOperator(
        task_id="load_postgres",
        python_callable=run_load_postgres,
    )

    load_redshift_task = PythonOperator(
        task_id="load_redshift",
        python_callable=run_load_redshift,
    )

    # Define el flujo de tareas con carga paralela
    extract_task >> transform_task >> [load_postgres_task, load_redshift_task]
