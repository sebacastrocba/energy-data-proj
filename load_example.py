"""
Script para cargar datos a PostgreSQL usando Docker.

Este script demuestra cómo:
1. Extraer datos
2. Transformarlos/limpiarlos
3. Cargarlos a PostgreSQL

Ejecutar: poetry run python load_example.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import pandas as pd
from fuel_price.load import load_fuel_data_to_postgres, get_db_connection

print("\n" + "=" * 70)
print("CARGANDO DATOS DE PRECIO DEL BRENT A POSTGRESQL")
print("=" * 70)

# PASO 1: LEER DATOS DEL CSV
print("\n📥 PASO 1: Leyendo datos del CSV...")
csv_path = Path(__file__).parent / 'data' / 'raw' / 'brent_prices.csv'
df_brent_raw = pd.read_csv(csv_path)
print(f"   ✓ Datos leídos: {len(df_brent_raw):,} registros")
print(f"   Columnas: {list(df_brent_raw.columns)}")

# PASO 2: LIMPIAR DATOS
print("\n🔧 PASO 2: Limpiando datos...")
df_brent = df_brent_raw.copy()
df_brent['date'] = pd.to_datetime(df_brent['date'])
df_brent = df_brent.rename(columns={'brent_price_usd': 'brent_price'})
df_brent = df_brent.dropna()
df_brent = df_brent.sort_values('date').reset_index(drop=True)
print(f"   ✓ Datos limpios: {len(df_brent):,} registros")
print(f"   Columnas: {list(df_brent.columns)}")
print(f"   Rango: {df_brent['date'].min()} a {df_brent['date'].max()}")

# Mostrar una muestra
print("\n   Muestra de datos:")
print(df_brent.head(3).to_string(index=False))

# PASO 3: CARGAR A POSTGRESQL
print("\n💾 PASO 3: Cargando a PostgreSQL...")
print("   (Esto puede tardar unos segundos...)")

total_rows = load_fuel_data_to_postgres(
    df=df_brent,
    table_name='brent_prices',
    schema='public',
    create_table=True,
    primary_key=['date'],
    indexes=[
        {'columns': ['date'], 'unique': True}
    ],
    method='copy'  # Método más rápido
)

print(f"\n   ✓ Datos cargados exitosamente")
print(f"   Total de registros en la tabla: {total_rows:,}")

# PASO 4: VERIFICAR LOS DATOS CARGADOS
print("\n🔍 PASO 4: Verificando datos en PostgreSQL...")

with get_db_connection() as conn:
    # Consultar los primeros registros
    query = "SELECT * FROM public.brent_prices ORDER BY date DESC LIMIT 5"
    df_from_db = pd.read_sql(query, conn)
    
    print("\n   Últimos 5 registros en la base de datos:")
    print(df_from_db.to_string(index=False))
    
    # Estadísticas
    stats_query = """
        SELECT 
            COUNT(*) as total_registros,
            MIN(date) as fecha_min,
            MAX(date) as fecha_max,
            ROUND(AVG(brent_price)::numeric, 2) as precio_promedio,
            ROUND(MIN(brent_price)::numeric, 2) as precio_min,
            ROUND(MAX(brent_price)::numeric, 2) as precio_max
        FROM public.brent_prices
    """
    df_stats = pd.read_sql(stats_query, conn)
    
    print("\n   Estadísticas de la tabla:")
    print(f"   - Total registros: {df_stats['total_registros'][0]:,}")
    print(f"   - Rango fechas: {df_stats['fecha_min'][0]} a {df_stats['fecha_max'][0]}")
    print(f"   - Precio promedio: ${df_stats['precio_promedio'][0]}")
    print(f"   - Precio mínimo: ${df_stats['precio_min'][0]}")
    print(f"   - Precio máximo: ${df_stats['precio_max'][0]}")

print("\n" + "=" * 70)
print("✅ PROCESO COMPLETADO EXITOSAMENTE")
print("=" * 70)

print("\n💡 COMANDOS ÚTILES:")
print("   Ver contenedor: docker-compose ps")
print("   Ver logs: docker-compose logs")
print("   Detener: docker-compose stop")
print("   Reiniciar: docker-compose restart")
print("   Eliminar: docker-compose down -v")
print("\n   Conectar con psql:")
print("   PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db")
print("=" * 70 + "\n")
