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

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Funciones de conexion


def get_postgres_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
):
    """
    Crea conexión a PostgreSQL.

    Args:
        host: Host de PostgreSQL (default: desde .env)
        port: Puerto (default: desde .env)
        database: Nombre de base de datos (default: desde .env)
        user: Usuario (default: desde .env)
        password: Contraseña (default: desde .env)

    Returns:
        Conexión psycopg2
    """
    connection_params = {
        "host": host or os.getenv("POSTGRES_HOST", "localhost"),
        "port": port or int(os.getenv("POSTGRES_PORT", 5432)),
        "database": database or os.getenv("POSTGRES_DB", "fuel_prices_db"),
        "user": user or os.getenv("POSTGRES_USER", "fuel_user"),
        "password": password or os.getenv("POSTGRES_PASSWORD", "fuel_password"),
    }

    try:
        host = connection_params["host"]
        port = connection_params["port"]
        db = connection_params["database"]
        logger.info(f"Conectando a PostgreSQL: {host}:{port}/{db}")
        conn = psycopg2.connect(**connection_params)
        logger.info("Conexion exitosa a PostgreSQL")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error al conectar a PostgreSQL: {e}")
        raise


def test_connection() -> bool:
    """
    Prueba la conexión a PostgreSQL.

    Returns:
        True si la conexión es exitosa, False en caso contrario
    """
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"Version de PostgreSQL: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error en test de conexion: {e}")
        return False


# Funciones de carga


def load_brent_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de Brent a staging.brent_prices.

    Args:
        df: DataFrame con columnas ['date', 'brent_price']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de Brent a staging")

    # Validar columnas requeridas
    required_cols = ["date", "brent_price"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame debe contener columnas: {required_cols}")

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        # Truncar tabla si se solicita
        if truncate:
            logger.info("Truncando tabla staging.brent_prices")
            cursor.execute(
                "TRUNCATE TABLE staging.brent_prices RESTART IDENTITY CASCADE;"
            )

        # Preparar datos para inserción (convertir tipos numpy a Python nativos)
        df_copy = df[["date", "brent_price"]].copy()
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date

        # Convertir usando values.tolist() que es mucho más rápido
        records_list = df_copy.values.tolist()

        # Insertar datos usando execute_values (más eficiente que insert por fila)
        insert_query = """
            INSERT INTO staging.brent_prices (date, brent_price_usd)
            VALUES %s
            ON CONFLICT (date) DO UPDATE
            SET brent_price_usd = EXCLUDED.brent_price_usd,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        conn.commit()
        rows_inserted = len(records_list)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en staging.brent_prices"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de Brent: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_fuel_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de combustibles a staging.fuel_prices.

    Args:
        df: DataFrame con datos de combustibles
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de combustibles a staging")

    # Validar columnas requeridas
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

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        if truncate:
            logger.info("Truncando tabla staging.fuel_prices")
            cursor.execute(
                "TRUNCATE TABLE staging.fuel_prices RESTART IDENTITY CASCADE;"
            )

        # Preparar datos
        logger.info(f"Preparando {len(df):,} registros para inserción...")
        df_copy = df[required_cols].copy()

        # Convertir periodo a date si es datetime
        if pd.api.types.is_datetime64_any_dtype(df_copy["periodo"]):
            df_copy["periodo"] = pd.to_datetime(df_copy["periodo"]).dt.date

        # Usar StringIO para COPY - mucho más rápido que INSERT
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

        logger.info("Ejecutando COPY para inserción masiva...")
        cols = ", ".join(required_cols)
        copy_sql = (
            f"COPY staging.fuel_prices ({cols}) FROM STDIN "
            "WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
        )
        cursor.copy_expert(sql=copy_sql, file=buffer)

        conn.commit()
        rows_inserted = len(df_copy)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en staging.fuel_prices"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de combustibles: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_usd_ars_to_staging(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de USD/ARS a staging.usd_ars_rates.

    Args:
        df: DataFrame con columnas ['date', 'usd_ars_oficial', 'usd_ars_blue', 'brecha_cambiaria_pct']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de USD/ARS a staging")

    # Validar columnas requeridas
    required_cols = ["date", "usd_ars_oficial", "usd_ars_blue"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame debe contener columnas: {required_cols}")

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        if truncate:
            logger.info("Truncando tabla staging.usd_ars_rates")
            cursor.execute(
                "TRUNCATE TABLE staging.usd_ars_rates RESTART IDENTITY CASCADE;"
            )

        # Preparar columnas (brecha_cambiaria_pct es opcional)
        cols_to_use = ["date", "usd_ars_oficial", "usd_ars_blue"]
        if "brecha_cambiaria_pct" in df.columns:
            cols_to_use.append("brecha_cambiaria_pct")

        # Convertir tipos numpy a Python nativos
        df_copy = df[cols_to_use].copy()
        df_copy["date"] = pd.to_datetime(df_copy["date"]).dt.date

        # Renombrar 'date' a 'fecha' para coincidir con la tabla
        df_copy = df_copy.rename(columns={"date": "fecha"})

        # Convertir usando values.tolist() que es mucho más rápido
        records_list = df_copy.values.tolist()

        # Construir query dinámicamente según columnas disponibles
        if "brecha_cambiaria_pct" in df.columns:
            insert_query = """
                INSERT INTO staging.usd_ars_rates
                (fecha, usd_ars_oficial, usd_ars_blue, brecha_cambiaria_pct)
                VALUES %s
                ON CONFLICT (fecha) DO UPDATE
                SET usd_ars_oficial = EXCLUDED.usd_ars_oficial,
                    usd_ars_blue = EXCLUDED.usd_ars_blue,
                    brecha_cambiaria_pct = EXCLUDED.brecha_cambiaria_pct,
                    load_timestamp = CURRENT_TIMESTAMP;
            """
        else:
            insert_query = """
                INSERT INTO staging.usd_ars_rates
                (fecha, usd_ars_oficial, usd_ars_blue)
                VALUES %s
                ON CONFLICT (fecha) DO UPDATE
                SET usd_ars_oficial = EXCLUDED.usd_ars_oficial,
                    usd_ars_blue = EXCLUDED.usd_ars_blue,
                    load_timestamp = CURRENT_TIMESTAMP;
            """

        execute_values(cursor, insert_query, records_list)

        conn.commit()
        rows_inserted = len(records_list)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en staging.usd_ars_rates"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de USD/ARS: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# Funciones de carga - Analytics


def load_brent_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de Brent agregados mensualmente a analytics.brent_prices_monthly.

    Args:
        df: DataFrame ya agregado mensualmente con columnas:
            ['year', 'month', 'avg_brent_price_usd', 'min_brent_price_usd',
             'max_brent_price_usd', 'record_count']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de Brent a analytics")

    required_cols = [
        "year",
        "month",
        "avg_brent_price_usd",
        "min_brent_price_usd",
        "max_brent_price_usd",
        "record_count",
    ]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Las agregaciones deben hacerse en transform.py"
        )

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        if truncate:
            logger.info("Truncando tabla analytics.brent_prices_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.brent_prices_monthly RESTART IDENTITY CASCADE;"
            )

        # Convertir tipos numpy a Python nativos
        df_copy = df[required_cols].copy()

        # Convertir usando values.tolist() que es mucho más rápido
        records_list = df_copy.values.tolist()

        insert_query = """
            INSERT INTO analytics.brent_prices_monthly
            (year, month, avg_brent_price_usd, min_brent_price_usd, max_brent_price_usd, record_count)
            VALUES %s
            ON CONFLICT (year, month) DO UPDATE
            SET avg_brent_price_usd = EXCLUDED.avg_brent_price_usd,
                min_brent_price_usd = EXCLUDED.min_brent_price_usd,
                max_brent_price_usd = EXCLUDED.max_brent_price_usd,
                record_count = EXCLUDED.record_count,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        conn.commit()
        rows_inserted = len(records_list)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en analytics.brent_prices_monthly"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de Brent analytics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_fuel_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de combustibles agregados a analytics.fuel_prices_monthly.

    Args:
        df: DataFrame ya agregado mensualmente con columnas:
            ['year', 'month', 'provincia', 'bandera', 'producto',
             'precio_surtidor_mediana', 'volumen_total']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de combustibles a analytics")

    required_cols = [
        "year",
        "month",
        "provincia",
        "bandera",
        "producto",
        "precio_surtidor_mediana",
        "volumen_total",
    ]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Las agregaciones deben hacerse en transform.py"
        )

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        if truncate:
            logger.info("Truncando tabla analytics.fuel_prices_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.fuel_prices_monthly RESTART IDENTITY CASCADE;"
            )

        # Convertir tipos numpy a Python nativos
        df_copy = df[required_cols].copy()

        # Convertir usando values.tolist() que es mucho más rápido
        records_list = df_copy.values.tolist()

        insert_query = """
            INSERT INTO analytics.fuel_prices_monthly
            (year, month, provincia, bandera, producto, precio_surtidor_mediana, volumen_total)
            VALUES %s
            ON CONFLICT (year, month, provincia, bandera, producto) DO UPDATE
            SET precio_surtidor_mediana = EXCLUDED.precio_surtidor_mediana,
                volumen_total = EXCLUDED.volumen_total,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        conn.commit()
        rows_inserted = len(records_list)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en analytics.fuel_prices_monthly"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de combustibles analytics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_usd_ars_to_analytics(df: pd.DataFrame, truncate: bool = True) -> int:
    """
    Carga datos de USD/ARS agregados a analytics.usd_ars_rates_monthly.

    Args:
        df: DataFrame ya agregado mensualmente con columnas:
            ['year', 'month', 'avg_usd_ars_oficial', 'avg_usd_ars_blue',
             'avg_brecha_cambiaria_pct', 'record_count']
        truncate: Si True, elimina datos existentes antes de cargar

    Returns:
        Cantidad de registros insertados
    """
    logger.info("Iniciando carga de USD/ARS a analytics")

    required_cols = [
        "year",
        "month",
        "avg_usd_ars_oficial",
        "avg_usd_ars_blue",
        "avg_brecha_cambiaria_pct",
        "record_count",
    ]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(
            f"DataFrame debe contener columnas: {required_cols}. "
            f"Las agregaciones deben hacerse en transform.py"
        )

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        if truncate:
            logger.info("Truncando tabla analytics.usd_ars_rates_monthly")
            cursor.execute(
                "TRUNCATE TABLE analytics.usd_ars_rates_monthly RESTART IDENTITY CASCADE;"
            )

        # Convertir tipos numpy a Python nativos
        df_copy = df[required_cols].copy()

        # Convertir usando values.tolist() que es mucho más rápido
        records_list = df_copy.values.tolist()

        insert_query = """
            INSERT INTO analytics.usd_ars_rates_monthly
            (year, month, avg_usd_ars_oficial, avg_usd_ars_blue, avg_brecha_cambiaria_pct, record_count)
            VALUES %s
            ON CONFLICT (year, month) DO UPDATE
            SET avg_usd_ars_oficial = EXCLUDED.avg_usd_ars_oficial,
                avg_usd_ars_blue = EXCLUDED.avg_usd_ars_blue,
                avg_brecha_cambiaria_pct = EXCLUDED.avg_brecha_cambiaria_pct,
                record_count = EXCLUDED.record_count,
                load_timestamp = CURRENT_TIMESTAMP;
        """

        execute_values(cursor, insert_query, records_list)

        conn.commit()
        rows_inserted = len(records_list)
        logger.info(
            f"Carga completada: {rows_inserted} registros insertados en analytics.usd_ars_rates_monthly"
        )

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error en carga de USD/ARS analytics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# Funcion principal


def load_all_data(
    brent_df: pd.DataFrame,
    fuel_df: pd.DataFrame,
    usd_ars_df: pd.DataFrame,
    load_to_analytics: bool = True,
):
    """
    Carga todos los datos a PostgreSQL (staging y opcionalmente analytics).

    Args:
        brent_df: DataFrame con datos de Brent
        fuel_df: DataFrame con datos de combustibles
        usd_ars_df: DataFrame con datos de USD/ARS
        load_to_analytics: Si True, también carga a tablas analytics
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
    rows_brent = load_brent_to_staging(brent_df)
    rows_fuel = load_fuel_to_staging(fuel_df)
    rows_usd = load_usd_ars_to_staging(usd_ars_df)

    logger.info(f"\nSTAGING - Resumen de carga:")
    logger.info(f"  - Brent: {rows_brent} registros")
    logger.info(f"  - Combustibles: {rows_fuel} registros")
    logger.info(f"  - USD/ARS: {rows_usd} registros")

    # Carga a ANALYTICS (opcional)
    if load_to_analytics:
        logger.info("\n[2/2] Cargando datos a ANALYTICS...")
        rows_brent_analytics = load_brent_to_analytics(brent_df)
        rows_fuel_analytics = load_fuel_to_analytics(fuel_df)
        rows_usd_analytics = load_usd_ars_to_analytics(usd_ars_df)

        logger.info(f"\nANALYTICS - Resumen de carga:")
        logger.info(f"  - Brent mensual: {rows_brent_analytics} registros")
        logger.info(f"  - Combustibles mensual: {rows_fuel_analytics} registros")
        logger.info(f"  - USD/ARS mensual: {rows_usd_analytics} registros")

    logger.info("\n" + "=" * 70)
    logger.info("CARGA COMPLETADA EXITOSAMENTE")
    logger.info("=" * 70)


# Script de prueba

if __name__ == "__main__":
    import sys

    sys.path.append(str(Path(__file__).parent.parent.parent))

    from src.fuel_price.extract import extract_all_data
    from src.fuel_price.transform import (
        clean_fuel_price,
        fuel_price_aggs,
        fuel_price_aggs_for_analytics,
        clean_brent_price,
        agg_brent_price_for_analytics,
        clean_dollar_price,
        dollar_price_aggs_for_analytics,
    )

    print("\n" + "=" * 70)
    print("PRUEBA DE CARGA A POSTGRESQL")
    print("=" * 70)

    # 1. Extraer datos
    print("\n[1/3] Extrayendo datos de APIs...")
    brent_raw, fuel_raw, dolar_raw = extract_all_data(update_all=False)

    # 2. Transformar datos
    print("\n[2/3] Transformando datos...")

    # Brent: limpiar y agregar para analytics
    brent_clean = clean_brent_price(brent_raw)
    brent_analytics = agg_brent_price_for_analytics(brent_clean)

    # Combustibles: limpiar y agregar
    fuel_clean = clean_fuel_price(fuel_raw)
    fuel_agg = fuel_price_aggs(fuel_clean)
    fuel_analytics = fuel_price_aggs_for_analytics(fuel_agg)

    # Dólar: limpiar y agregar para analytics
    dolar_clean = clean_dollar_price(dolar_raw)
    dolar_analytics = dollar_price_aggs_for_analytics(dolar_clean)

    # 3. Cargar datos a STAGING
    print("\n[3/4] Cargando datos a STAGING...")
    rows_brent = load_brent_to_staging(brent_clean)
    rows_fuel = load_fuel_to_staging(fuel_clean)
    rows_usd = load_usd_ars_to_staging(dolar_clean)

    print(f"\nSTAGING - Resumen de carga:")
    print(f"  - Brent: {rows_brent} registros")
    print(f"  - Combustibles: {rows_fuel} registros")
    print(f"  - USD/ARS: {rows_usd} registros")

    # 4. Cargar datos a ANALYTICS
    print("\n[4/4] Cargando datos a ANALYTICS...")
    rows_brent_analytics = load_brent_to_analytics(brent_analytics)
    rows_fuel_analytics = load_fuel_to_analytics(fuel_analytics)
    rows_usd_analytics = load_usd_ars_to_analytics(dolar_analytics)

    print(f"\nANALYTICS - Resumen de carga:")
    print(f"  - Brent mensual: {rows_brent_analytics} registros")
    print(f"  - Combustibles mensual: {rows_fuel_analytics} registros")
    print(f"  - USD/ARS mensual: {rows_usd_analytics} registros")

    print("\n" + "=" * 70)
    print("CARGA COMPLETADA EXITOSAMENTE")
    print("=" * 70)
    print("\nPRUEBA COMPLETADA")
