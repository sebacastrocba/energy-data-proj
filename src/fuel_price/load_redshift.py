"""
Modulo para cargar datos a AWS Redshift.
Usa psycopg2 con connection string directa.
"""

import psycopg2
import psycopg2.extras
import pandas as pd
from pathlib import Path
import logging
import os
from contextlib import contextmanager
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Schema personal asignado por el curso
REDSHIFT_SCHEMA = "2025_sebastian_castro_schema"


@contextmanager
def get_redshift_connection():
    """
    Context manager para conexiones a AWS Redshift.

    Lee la connection string desde REDSHIFT_CONNECTION_STRING en .env:
    postgresql://user:pass@cluster.xxx.redshift.amazonaws.com:5439/pda

    Yields:
        tuple: (conexión, cursor) listos para usar
    """
    conn = None
    cursor = None

    try:
        conn_str = os.getenv("REDSHIFT_CONNECTION_STRING")
        if not conn_str:
            raise ValueError("REDSHIFT_CONNECTION_STRING no configurada en .env")

        logger.info("Conectando a Redshift...")

        # psycopg2 acepta directamente la connection string
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        logger.info("ConexiÃ³n exitosa a Redshift")

        yield conn, cursor

        conn.commit()
        logger.debug("Commit exitoso")

    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Rollback ejecutado: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.debug("ConexiÃ³n cerrada")


def test_redshift_connection() -> bool:
    """Prueba la conexiÃ³n a Redshift."""
    try:
        with get_redshift_connection() as (conn, cursor):
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Redshift version: {version[0][:50]}...")
        return True
    except Exception as e:
        logger.error(f"Error en test de conexiÃ³n: {e}")
        return False


def verify_schema_exists():
    """Verifica que el schema personal exista."""
    logger.info(f"Verificando schema: {REDSHIFT_SCHEMA}")

    with get_redshift_connection() as (conn, cursor):
        cursor.execute(
            """
            SELECT nspname 
            FROM pg_namespace 
            WHERE nspname = %s;
        """,
            (REDSHIFT_SCHEMA,),
        )

        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Schema '{REDSHIFT_SCHEMA}' no existe en Redshift")

        logger.info(f"Schema {REDSHIFT_SCHEMA} verificado exitosamente")


def create_staging_tables():
    """Crea las tablas en el schema personal con prefijo staging_."""
    logger.info(f"Creando tablas staging en {REDSHIFT_SCHEMA}...")

    with get_redshift_connection() as (conn, cursor):

        # Borrar TODAS las tablas staging existentes para recrearlas con nueva estructura
        logger.info("Borrando tablas staging existentes...")
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".staging_brent_price CASCADE;'
        )
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".staging_fuel_prices CASCADE;'
        )
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".staging_usd_ars_rates CASCADE;'
        )

        # Commit explícito para limpiar metadata de Redshift
        conn.commit()
        logger.info("Metadata limpiada - procediendo a crear tablas...")

        # Tabla: staging_brent_price
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".staging_brent_price (
                date DATE NOT NULL,
                brent_price FLOAT NOT NULL,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (date)
            ) DISTSTYLE ALL
            SORTKEY (date);
        """
        )
        logger.info("  - staging_brent_price creada")

        # Tabla: staging_fuel_prices
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".staging_fuel_prices (
                id BIGINT IDENTITY(1,1),
                periodo DATE NOT NULL,
                operador VARCHAR(200),
                nro_inscripcion VARCHAR(50),
                bandera VARCHAR(100),
                fecha_de_baja DATE,
                cuit VARCHAR(20),
                tipo_negocio VARCHAR(100),
                direccion VARCHAR(300),
                localidad VARCHAR(200),
                provincia VARCHAR(100),
                producto VARCHAR(100) NOT NULL,
                canal_de_comercializacion VARCHAR(100),
                precio_sin_impuestos FLOAT,
                precio_con_impuestos FLOAT,
                volumen FLOAT,
                precio_surtidor FLOAT NOT NULL,
                no_movimientos VARCHAR(50),
                excentos FLOAT,
                impuesto_combustible_liquido FLOAT,
                impuesto_dioxido_carbono FLOAT,
                tasa_vial FLOAT,
                tasa_municipal FLOAT,
                ingresos_brutos FLOAT,
                iva FLOAT,
                fondo_fiduciario_gnc FLOAT,
                impuesto_combustible_liquidos FLOAT,
                market_share_pct FLOAT,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (id)
            ) DISTSTYLE KEY
            DISTKEY (producto)
            SORTKEY (periodo, producto);
        """
        )
        logger.info("  - staging_fuel_prices creada")

        # Tabla: staging_usd_ars_rates
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".staging_usd_ars_rates (
                date DATE NOT NULL,
                source VARCHAR(20) NOT NULL,
                value_buy FLOAT,
                value_sell FLOAT,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (date, source)
            ) DISTSTYLE ALL
            SORTKEY (date, source);
        """
        )
        logger.info("  - staging_usd_ars_rates creada")

        logger.info("Tablas staging creadas exitosamente")


def create_analytics_tables():
    """Crea las tablas analytics en el schema personal con prefijo analytics_."""
    logger.info(f"Creando tablas analytics en {REDSHIFT_SCHEMA}...")

    with get_redshift_connection() as (conn, cursor):

        # Borrar TODAS las tablas analytics existentes para recrearlas
        logger.info("Borrando tablas analytics existentes...")
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".analytics_brent_prices_monthly CASCADE;'
        )
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".analytics_fuel_prices_monthly CASCADE;'
        )
        cursor.execute(
            f'DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}".analytics_usd_ars_rates_monthly CASCADE;'
        )

        # Commit explícito para limpiar metadata de Redshift
        conn.commit()
        logger.info("Metadata limpiada - procediendo a crear tablas...")

        # Tabla: analytics_brent_prices_monthly
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".analytics_brent_prices_monthly (
                date DATE NOT NULL,
                avg_brent_price FLOAT NOT NULL,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (date)
            ) DISTSTYLE ALL
            SORTKEY (date);
        """
        )
        logger.info("  - analytics_brent_prices_monthly creada")

        # Tabla: analytics_fuel_prices_monthly
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".analytics_fuel_prices_monthly (
                periodo DATE NOT NULL,
                producto VARCHAR(100) NOT NULL,
                precio_surtidor_mediana FLOAT NOT NULL,
                volumen_total FLOAT,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (periodo, producto)
            ) DISTSTYLE KEY
            DISTKEY (producto)
            SORTKEY (periodo, producto);
        """
        )
        logger.info("  - analytics_fuel_prices_monthly creada")

        # Tabla: analytics_usd_ars_rates_monthly
        cursor.execute(
            f"""
            CREATE TABLE "{REDSHIFT_SCHEMA}".analytics_usd_ars_rates_monthly (
                date DATE NOT NULL,
                usd_ars_oficial FLOAT NOT NULL,
                usd_ars_blue FLOAT NOT NULL,
                brecha_cambiaria_pct FLOAT,
                load_timestamp TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (date)
            ) DISTSTYLE ALL
            SORTKEY (date);
        """
        )
        logger.info("  - analytics_usd_ars_rates_monthly creada")

        logger.info("Tablas analytics creadas exitosamente")


def create_all_tables():
    """Verifica el schema y crea todas las tablas necesarias."""
    logger.info("=" * 70)
    logger.info("INICIALIZANDO ESTRUCTURA EN REDSHIFT")
    logger.info("=" * 70)

    verify_schema_exists()
    create_staging_tables()
    create_analytics_tables()

    logger.info("=" * 70)
    logger.info("ESTRUCTURA CREADA EXITOSAMENTE")
    logger.info("=" * 70)


def load_to_redshift(
    df: pd.DataFrame, table: str, table_type: str = "staging", truncate: bool = True
) -> int:
    """
    Carga DataFrame a una tabla de Redshift.

    Args:
        df: DataFrame con los datos
        table: Nombre base de la tabla (sin prefijo)
        table_type: Tipo de tabla - "staging" o "analytics" (para el prefijo)
        truncate: Si True, limpia la tabla antes de cargar

    Returns:
        int: Cantidad de registros insertados
    """
    if df.empty:
        logger.warning(f"DataFrame vacío, no se cargará nada a {table}")
        return 0

    # Construir nombre completo con prefijo - IMPORTANTE: schema y tabla van entre comillas separadas
    full_table_name = f'"{REDSHIFT_SCHEMA}"."{table_type}_{table}"'

    logger.info(f"Cargando {len(df):,} registros a {full_table_name}")

    with get_redshift_connection() as (conn, cursor):

        # Truncar tabla si se solicita
        if truncate:
            logger.info(f"Truncando {full_table_name}...")
            cursor.execute(f"TRUNCATE TABLE {full_table_name};")

        # Preparar datos
        df_copy = df.copy()

        # Convertir columnas datetime a date
        for col in df_copy.select_dtypes(include=["datetime64"]).columns:
            df_copy[col] = pd.to_datetime(df_copy[col]).dt.date

        # Manejar fecha_de_baja si existe (estÃ¡ en formato string malformado)
        # La convertimos a None ya que no es crÃ­tica para el anÃ¡lisis
        if "fecha_de_baja" in df_copy.columns:
            logger.debug("Limpiando columna fecha_de_baja (datos malformados)")
            df_copy["fecha_de_baja"] = None

        # NUEVO: Definir columnas válidas por tabla
        valid_columns = {
            "staging_brent_price": ["date", "brent_price"],
            "staging_fuel_prices": [
                "periodo",
                "operador",
                "nro_inscripcion",
                "bandera",
                "fecha_de_baja",
                "cuit",
                "tipo_negocio",
                "direccion",
                "localidad",
                "provincia",
                "producto",
                "canal_de_comercializacion",
                "precio_sin_impuestos",
                "precio_con_impuestos",
                "volumen",
                "precio_surtidor",
                "no_movimientos",
                "excentos",
                "impuesto_combustible_liquido",
                "impuesto_dioxido_carbono",
                "tasa_vial",
                "tasa_municipal",
                "ingresos_brutos",
                "iva",
                "fondo_fiduciario_gnc",
                "impuesto_combustible_liquidos",
                "market_share_pct",
            ],
            "staging_usd_ars_rates": ["date", "source", "value_buy", "value_sell"],
            "analytics_brent_prices_monthly": ["date", "avg_brent_price"],
            "analytics_fuel_prices_monthly": [
                "periodo",
                "producto",
                "precio_surtidor_mediana",
                "volumen_total",
            ],
            "analytics_usd_ars_rates_monthly": [
                "date",
                "usd_ars_oficial",
                "usd_ars_blue",
                "brecha_cambiaria_pct",
            ],
        }

        # Filtrar solo las columnas válidas para esta tabla
        table_key = f"{table_type}_{table}"
        if table_key in valid_columns:
            available_cols = [
                col for col in valid_columns[table_key] if col in df_copy.columns
            ]
            df_copy = df_copy[available_cols]
            logger.info(
                f"  Columnas seleccionadas: {len(available_cols)} de {len(valid_columns[table_key])} definidas"
            )

        columns = list(df_copy.columns)
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"INSERT INTO {full_table_name} ({cols_str}) VALUES %s"

        # Preparar valores como lista de tuplas
        logger.info(f"  Insertando {len(df_copy):,} registros...")
        values = [tuple(row) for row in df_copy.values]

        # Usar execute_values (más eficiente que executemany)
        psycopg2.extras.execute_values(cursor, query, values, page_size=1000)

        logger.info(f"Carga completada: {len(df_copy):,} registros insertados")

        return len(df_copy)


def load_all_data_to_redshift(
    brent_clean: pd.DataFrame,
    fuel_clean: pd.DataFrame,
    usd_ars_clean: pd.DataFrame,
    brent_analytics: pd.DataFrame,
    fuel_analytics: pd.DataFrame,
    usd_ars_analytics: pd.DataFrame,
):
    """Carga todos los datos a Redshift usando el schema personal."""
    logger.info("=" * 70)
    logger.info("INICIANDO CARGA COMPLETA A REDSHIFT")
    logger.info(f"Schema destino: {REDSHIFT_SCHEMA}")
    logger.info("=" * 70)

    # Test de conexiÃ³n
    if not test_redshift_connection():
        raise ConnectionError("No se puede conectar a Redshift")

    # Crear estructura
    create_all_tables()

    # Carga a STAGING
    logger.info("\n[1/2] Cargando datos a STAGING...")
    rows_brent = load_to_redshift(brent_clean, "brent_price", "staging")
    rows_fuel = load_to_redshift(fuel_clean, "fuel_prices", "staging")
    rows_usd = load_to_redshift(usd_ars_clean, "usd_ars_rates", "staging")

    logger.info(f"\nSTAGING - Resumen:")
    logger.info(f"  - Brent: {rows_brent:,} registros")
    logger.info(f"  - Combustibles: {rows_fuel:,} registros")
    logger.info(f"  - USD/ARS: {rows_usd:,} registros")

    # Carga a ANALYTICS
    logger.info("\n[2/2] Cargando datos a ANALYTICS...")
    rows_brent_analytics = load_to_redshift(
        brent_analytics, "brent_prices_monthly", "analytics"
    )
    rows_fuel_analytics = load_to_redshift(
        fuel_analytics, "fuel_prices_monthly", "analytics"
    )
    rows_usd_analytics = load_to_redshift(
        usd_ars_analytics, "usd_ars_rates_monthly", "analytics"
    )

    logger.info(f"\nANALYTICS - Resumen:")
    logger.info(f"  - Brent mensual: {rows_brent_analytics:,} registros")
    logger.info(f"  - Combustibles mensual: {rows_fuel_analytics:,} registros")
    logger.info(f"  - USD/ARS mensual: {rows_usd_analytics:,} registros")

    logger.info("\n" + "=" * 70)
    logger.info("CARGA A REDSHIFT COMPLETADA")
    logger.info("=" * 70)


# Script de prueba standalone
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("SCRIPT DE PRUEBA - CARGA A REDSHIFT")
    logger.info("=" * 70)

    # Test de conexión
    logger.info("\nProbando conexión...")
    if test_redshift_connection():
        logger.info("✓ Conexión exitosa")
    else:
        logger.error("✗ No se pudo conectar")
        import sys

        sys.exit(1)

    # Crear estructura
    logger.info("\nCreando estructura...")
    create_all_tables()

    # Cargar datos
    project_root = Path(__file__).parent.parent.parent
    processed_path = project_root / "data" / "processed"

    logger.info(f"\nLeyendo datos desde: {processed_path}")

    required_files = {
        "brent_cleaned": processed_path / "brent_price_cleaned.parquet",
        "brent_monthly": processed_path / "brent_price_monthly.parquet",
        "fuel_cleaned": processed_path / "fuel_price_cleaned.parquet",
        "fuel_aggregated": processed_path / "fuel_price_aggregated.parquet",
        "dollar_cleaned": processed_path / "dollar_price_cleaned.parquet",
        "dollar_aggregated": processed_path / "dollar_price_aggregated.parquet",
    }

    missing = [str(f) for f in required_files.values() if not f.exists()]
    if missing:
        logger.error("\nArchivos faltantes:")
        for f in missing:
            logger.error(f"  - {f}")
        logger.error("\nEjecuta transform.py primero")
        import sys

        sys.exit(1)

    # Cargar parquets
    logger.info("\nCargando archivos parquet...")
    brent_clean = pd.read_parquet(required_files["brent_cleaned"])
    brent_analytics = pd.read_parquet(required_files["brent_monthly"])
    fuel_clean = pd.read_parquet(required_files["fuel_cleaned"])
    fuel_analytics = pd.read_parquet(required_files["fuel_aggregated"])
    usd_ars_clean = pd.read_parquet(required_files["dollar_cleaned"])
    usd_ars_analytics = pd.read_parquet(required_files["dollar_aggregated"])

    logger.info("✓ Archivos cargados")

    # Ejecutar carga
    try:
        load_all_data_to_redshift(
            brent_clean=brent_clean,
            fuel_clean=fuel_clean,
            usd_ars_clean=usd_ars_clean,
            brent_analytics=brent_analytics,
            fuel_analytics=fuel_analytics,
            usd_ars_analytics=usd_ars_analytics,
        )

        logger.info("\n“ PRUEBA COMPLETADA EXITOSAMENTE")

    except Exception as e:
        logger.error(f"\n— ERROR: {e}", exc_info=True)
        import sys

        sys.exit(1)
