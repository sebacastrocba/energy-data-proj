import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from pathlib import Path
from typing import Optional, Dict, List
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from contextlib import contextmanager

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 1. Creo el Context Manager


@contextmanager
def get_db_connection():
    """
    Context manager para conexiones PostgreSQL.
    Maneja automáticamente apertura, commit/rollback y cierre.

    Uso:
        with get_db_connection() as (conn, cursor):
            cursor.execute("INSERT ...")
            # commit automático al salir del bloque

    Yields:
        tuple: (conexión, cursor) listos para usar
    """
    conn = None
    cursor = None

    try:
        # 1. CONECTAR
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        database = os.getenv("POSTGRES_DB", "fuel_prices_db")
        user = os.getenv("POSTGRES_USER", "fuel_user")
        password = os.getenv("POSTGRES_PASSWORD", "fuel_password")

        logger.info(f"Conectando a PostgreSQL: {host}:{port}/{database}")

        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        cursor = conn.cursor()
        logger.info("Conexion exitosa a PostgreSQL")

        # 2. YIELD
        yield conn, cursor

        # 3. COMMIT
        conn.commit()
        logger.debug("Commit exitoso")

    except Exception as e:
        # 4. ROLLBACK
        if conn:
            conn.rollback()
            logger.error(f"Rollback ejecutado debido a error: {e}")
        raise

    finally:
        # 5. CERRAR
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.debug("Conexión cerrada")


def test_connection() -> bool:
    """
    Prueba la conexión a PostgreSQL.

    Returns:
        True si la conexión es exitosa, False en caso contrario
    """
    try:
        with get_db_connection() as (conn, cursor):
            cursor.execute("SELECT version();")
            version = cursor.fetchone()

        return True

    except Exception as e:
        logger.error(f"Error en test de conexion: {e}")
        return False


# 2. Funciones de carga

## 2.1 Cargamos datos del precio del Brent


def load_brent_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de Brent a staging.brent_price.

    Args:
        df: DataFrame con columnas ['date', 'brent_price']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de Brent a staging")

    # 1. VALIDACIÓN
    required_cols = ["date", "brent_price"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame debe contener columnas: {required_cols}")

    # 2. USAR CONTEXT MANAGER
    with get_db_connection() as (conn, cursor):

        # 3. TRUNCAR (si se solicita)
        if truncate:
            logger.info("Truncando tabla staging.brent_price")
            cursor.execute(
                "TRUNCATE TABLE staging.brent_price RESTART IDENTITY CASCADE;"
            )

        # 4. PREPARAR DATOS
        df_copy = df[["date", "brent_price"]].copy()
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date
        records_list = df_copy.values.tolist()

        # 5. INSERTAR DATOS
        insert_query = """
            INSERT INTO staging.brent_price (date, brent_price)
            VALUES %s
            ON CONFLICT (date) DO UPDATE
            SET brent_price = EXCLUDED.brent_price,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        logger.info(
            f"Carga completada: {len(records_list)} registros insertados en staging.brent_price"
        )

        return len(records_list)


## 2.2 Cargamos datos de precios de combustibles


def load_fuel_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de combustibles a staging.fuel_prices usando COPY.

    Args:
        df: DataFrame con datos de combustibles
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de datos de la SE a staging")

    # VALIDACIÓN
    required_cols = [
        "periodo",
        "provincia",
        "bandera",
        "producto",
        "precio_surtidor",
        "volumen",
    ]

    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame debe contener columnas: {required_cols}")

    # CONTEXT MANAGER
    with get_db_connection() as (conn, cursor):

        if truncate:
            logger.info("Truncando tabla staging.fuel_prices")
            cursor.execute(
                "TRUNCATE TABLE staging.fuel_prices RESTART IDENTITY CASCADE;"
            )

        # Preparar datos para COPY
        logger.info(f"Preparando {len(df):,} registros para inserción...")
        df_copy = df[required_cols].copy()

        if pd.api.types.is_datetime64_any_dtype(df_copy["periodo"]):
            df_copy["periodo"] = pd.to_datetime(df_copy["periodo"]).dt.date

        # Usar StringIO para COPY
        from io import StringIO
        import csv

        buffer = StringIO()
        df_copy.to_csv(
            buffer,
            index=False,
            header=False,
            sep="\t",
            na_rep="\\N",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )

        buffer.seek(0)

        # Ejecutar COPY
        logger.info("Ejecutando COPY para inserción masiva...")
        cols = ", ".join(required_cols)
        copy_sql = (
            f"COPY staging.fuel_prices ({cols}) FROM STDIN "
            "WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
        )
        cursor.copy_expert(sql=copy_sql, file=buffer)

        # Commit automático al salir del with
        logger.info(
            f"Carga completada: {len(df_copy)} registros insertados en staging.fuel_prices"
        )
        return len(df_copy)


## 2.3 Cargamos datos de dolar blue y oficial


def load_dolar_price_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de USD/ARS a staging.usd_ars_rates.

    Args:
        df: DataFrame con columnas ['date', 'source', 'value_buy', 'value_sell']
            (datos SIN pivotar del archivo limpio)
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de USD/ARS a staging")

    # VALIDACIÓN - Columnas del archivo LIMPIO (sin pivotar)
    required_cols = ["date", "source", "value_sell"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Columnas recibidas: {df.columns.tolist()}"
        )

    with get_db_connection() as (conn, cursor):
        if truncate:
            logger.info("Truncando tabla staging.usd_ars_rates")
            cursor.execute(
                "TRUNCATE TABLE staging.usd_ars_rates RESTART IDENTITY CASCADE;"
            )

        # Preparar datos SIN pivotar ni agregar
        cols_to_use = ["date", "source", "value_buy", "value_sell"]
        df_copy = df[cols_to_use].copy()
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date

        records_list = df_copy.values.tolist()

        # INSERT con estructura sin pivotar
        insert_query = """
            INSERT INTO staging.usd_ars_rates
            (date, source, value_buy, value_sell)
            VALUES %s
            ON CONFLICT (date, source) DO UPDATE
            SET value_buy = EXCLUDED.value_buy,
                value_sell = EXCLUDED.value_sell,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        logger.info(
            f"Carga completada: {len(records_list)} registros insertados en staging.usd_ars_rates"
        )
        return len(records_list)


# Funciones de carga - Analytics

## Cargamos datos agregados del Brent


def load_brent_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de Brent agregados mensualmente a analytics.

    Args:
        df: DataFrame con columnas ['date', 'avg_brent_price']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de Brent a analytics")

    # VALIDACIÓN - Verificar columnas requeridas
    required_cols = ["date", "avg_brent_price"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Columnas recibidas: {df.columns.tolist()}"
        )

    with get_db_connection() as (conn, cursor):
        if truncate:
            logger.info("Truncando tabla analytics.brent_prices_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.brent_prices_monthly RESTART IDENTITY CASCADE;"
            )

        # Preparar datos
        df_copy = df[required_cols].copy()

        # Convertir date a formato date (sin hora)
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date

        records_list = df_copy.values.tolist()

        # INSERT con las columnas correctas
        insert_query = """
            INSERT INTO analytics.brent_prices_monthly
            (date, avg_brent_price)
            VALUES %s
            ON CONFLICT (date) DO UPDATE
            SET avg_brent_price = EXCLUDED.avg_brent_price,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        logger.info(
            f"Carga completada: {len(records_list)} registros en analytics.brent_prices_monthly"
        )
        return len(records_list)


def load_fuel_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de precios de combustibles agregados de la SE a analytics.fuel_prices_monthly.

    Args:
        df: DataFrame ya agregado mensualmente con columnas:
            ['periodo', 'producto', 'precio_surtidor_mediana', 'volumen_total']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de combustibles SE a analytics")

    # VALIDACIÓN - Verificar columnas requeridas
    required_cols = [
        "periodo",
        "producto",
        "precio_surtidor_mediana",
        "volumen_total",
    ]

    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Columnas recibidas: {df.columns.tolist()}"
        )

    # CONTEXT MANAGER
    with get_db_connection() as (conn, cursor):

        if truncate:
            logger.info("Truncando tabla analytics.fuel_prices_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.fuel_prices_monthly RESTART IDENTITY CASCADE;"
            )

        # Preparar datos
        df_copy = df[required_cols].copy()

        # Convertir periodo a date (sin hora)
        df_copy["periodo"] = pd.to_datetime(df_copy["periodo"]).dt.date

        records_list = df_copy.values.tolist()

        # INSERT con las columnas correctas
        insert_query = """
            INSERT INTO analytics.fuel_prices_monthly
            (periodo, producto, precio_surtidor_mediana, volumen_total)
            VALUES %s
            ON CONFLICT (periodo, producto) DO UPDATE
            SET precio_surtidor_mediana = EXCLUDED.precio_surtidor_mediana,
                volumen_total = EXCLUDED.volumen_total,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        logger.info(
            f"Carga completada: {len(records_list)} registros insertados en analytics.fuel_prices_monthly"
        )

        return len(records_list)


def load_dolar_price_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de USD/ARS agregados a analytics.usd_ars_rates_monthly.

    Args:
        df: DataFrame con columnas ['date', 'usd_ars_oficial', 'usd_ars_blue', 'brecha_cambiaria_pct']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de USD/ARS a analytics")

    required_cols = ["date", "usd_ars_oficial", "usd_ars_blue"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Columnas recibidas: {df.columns.tolist()}"
        )

    with get_db_connection() as (conn, cursor):
        if truncate:
            logger.info("Truncando tabla analytics.usd_ars_rates_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.usd_ars_rates_monthly RESTART IDENTITY CASCADE;"
            )

        # Preparar columnas (con o sin brecha)
        cols_to_use = ["date", "usd_ars_oficial", "usd_ars_blue"]
        if "brecha_cambiaria_pct" in df.columns:
            cols_to_use.append("brecha_cambiaria_pct")

        df_copy = df[cols_to_use].copy()
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date
        records_list = df_copy.values.tolist()

        # Query adaptado según columnas disponibles
        if "brecha_cambiaria_pct" in df.columns:
            insert_query = """
                INSERT INTO analytics.usd_ars_rates_monthly
                (date, usd_ars_oficial, usd_ars_blue, brecha_cambiaria_pct)
                VALUES %s
                ON CONFLICT (date) DO UPDATE
                SET usd_ars_oficial = EXCLUDED.usd_ars_oficial,
                    usd_ars_blue = EXCLUDED.usd_ars_blue,
                    brecha_cambiaria_pct = EXCLUDED.brecha_cambiaria_pct,
                    load_timestamp = CURRENT_TIMESTAMP;
            """
        else:
            insert_query = """
                INSERT INTO analytics.usd_ars_rates_monthly
                (date, usd_ars_oficial, usd_ars_blue)
                VALUES %s
                ON CONFLICT (date) DO UPDATE
                SET usd_ars_oficial = EXCLUDED.usd_ars_oficial,
                    usd_ars_blue = EXCLUDED.usd_ars_blue,
                    load_timestamp = CURRENT_TIMESTAMP;
            """

        execute_values(cursor, insert_query, records_list)
        logger.info(
            f"Carga completada: {len(records_list)} registros en analytics.usd_ars_rates_monthly"
        )
        return len(records_list)


# Funcion principal


def load_all_data(
    brent_clean: pd.DataFrame,
    fuel_clean: pd.DataFrame,
    usd_ars_clean: pd.DataFrame,
    brent_analytics: pd.DataFrame,
    fuel_analytics: pd.DataFrame,
    usd_ars_analytics: pd.DataFrame,
):
    """
    Carga todos los datos a PostgreSQL (staging y analytics).

    Args:
        brent_clean: DataFrame con datos de Brent limpios para staging
        fuel_clean: DataFrame con datos de combustibles limpios para staging
        usd_ars_clean: DataFrame con datos de USD/ARS limpios para staging
        brent_analytics: DataFrame con datos de Brent agregados para analytics
        fuel_analytics: DataFrame con datos de combustibles agregados para analytics
        usd_ars_analytics: DataFrame con datos de USD/ARS agregados para analytics
    """
    logger.info("=" * 70)
    logger.info("INICIANDO CARGA COMPLETA A POSTGRESQL")
    logger.info("=" * 70)

    # Test de conexión
    if not test_connection():
        raise ConnectionError(
            "No se puede conectar a PostgreSQL. Verifica que Docker este corriendo."
        )

    # Carga a STAGING
    logger.info("\n[1/2] Cargando datos a STAGING...")
    rows_brent = load_brent_to_staging(brent_clean)
    rows_fuel = load_fuel_to_staging(fuel_clean)
    rows_usd = load_dolar_price_to_staging(usd_ars_clean)

    logger.info(f"\nSTAGING - Resumen de carga:")
    logger.info(f"  - Brent: {rows_brent} registros")
    logger.info(f"  - Combustibles: {rows_fuel} registros")
    logger.info(f"  - USD/ARS: {rows_usd} registros")

    # Carga a ANALYTICS
    logger.info("\n[2/2] Cargando datos a ANALYTICS...")
    rows_brent_analytics = load_brent_to_analytics(brent_analytics)
    rows_fuel_analytics = load_fuel_to_analytics(fuel_analytics)
    rows_usd_analytics = load_dolar_price_to_analytics(usd_ars_analytics)

    logger.info(f"\nANALYTICS - Resumen de carga:")
    logger.info(f"  - Brent mensual: {rows_brent_analytics} registros")
    logger.info(f"  - Combustibles mensual: {rows_fuel_analytics} registros")
    logger.info(f"  - USD/ARS mensual: {rows_usd_analytics} registros")

    logger.info("\n" + "=" * 70)
    logger.info("CARGA COMPLETADA EXITOSAMENTE")
    logger.info("=" * 70)


# Script de prueba


if __name__ == "__main__":

    logger.info("=" * 70)
    logger.info("SCRIPT DE PRUEBA - CARGA DE DATOS A POSTGRESQL")
    logger.info("=" * 70)

    # Obtener rutas a los archivos procesados
    project_root = Path(__file__).parent.parent.parent
    processed_path = project_root / "data" / "processed"

    logger.info(f"Directorio de datos procesados: {processed_path}")

    # Verificar que existan los archivos
    required_files = {
        "brent_cleaned": processed_path / "brent_price_cleaned.parquet",
        "brent_monthly": processed_path / "brent_price_monthly.parquet",
        "fuel_cleaned": processed_path / "fuel_price_cleaned.parquet",
        "fuel_aggregated": processed_path / "fuel_price_aggregated.parquet",
        "dollar_cleaned": processed_path / "dollar_price_cleaned.parquet",
        "dollar_aggregated": processed_path / "dollar_price_aggregated.parquet",
    }

    missing_files = []
    for name, filepath in required_files.items():
        if not filepath.exists():
            missing_files.append(str(filepath))
            logger.warning(f"Archivo no encontrado: {filepath}")

    if missing_files:
        logger.error("\n" + "=" * 70)
        logger.error("ERROR: Faltan archivos procesados")
        logger.error("=" * 70)
        logger.error("Archivos faltantes:")
        for f in missing_files:
            logger.error(f"  - {f}")
        logger.error(
            "\nEjecuta primero el script transform.py para generar los archivos."
        )
        import sys

        sys.exit(1)

    # Cargar los DataFrames desde Parquet
    logger.info("\nCargando archivos Parquet...")

    brent_clean = pd.read_parquet(required_files["brent_cleaned"])
    brent_analytics = pd.read_parquet(required_files["brent_monthly"])

    fuel_clean = pd.read_parquet(required_files["fuel_cleaned"])
    fuel_analytics = pd.read_parquet(required_files["fuel_aggregated"])

    usd_ars_clean = pd.read_parquet(required_files["dollar_cleaned"])
    usd_ars_analytics = pd.read_parquet(required_files["dollar_aggregated"])

    logger.info("\nArchivos cargados exitosamente:")
    logger.info(f"  - Brent cleaned: {len(brent_clean):,} registros")
    logger.info(f"  - Brent monthly: {len(brent_analytics):,} registros")
    logger.info(f"  - Fuel cleaned: {len(fuel_clean):,} registros")
    logger.info(f"  - Fuel aggregated: {len(fuel_analytics):,} registros")
    logger.info(f"  - USD/ARS cleaned: {len(usd_ars_clean):,} registros")
    logger.info(f"  - USD/ARS aggregated: {len(usd_ars_analytics):,} registros")

    # Ejecutar carga completa
    try:
        load_all_data(
            brent_clean=brent_clean,
            fuel_clean=fuel_clean,
            usd_ars_clean=usd_ars_clean,
            brent_analytics=brent_analytics,
            fuel_analytics=fuel_analytics,
            usd_ars_analytics=usd_ars_analytics,
        )

        logger.info("\n" + "=" * 70)
        logger.info("✓ PRUEBA COMPLETADA EXITOSAMENTE")
        logger.info("=" * 70)

    except Exception as e:
        logger.error("\n" + "=" * 70)
        logger.error("✗ ERROR EN LA CARGA")
        logger.error("=" * 70)
        logger.error(f"Error: {e}")
        import sys

        sys.exit(1)
