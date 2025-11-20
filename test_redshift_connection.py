"""
Script temporal para testear conexión a Redshift
Borralo después de verificar que funciona
"""

import psycopg2
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener connection string
conn_str = os.getenv("REDSHIFT_CONNECTION_STRING")

if not conn_str:
    print("ERROR: Variable REDSHIFT_CONNECTION_STRING no encontrada en .env")
    print("Agregala así:")
    print('REDSHIFT_CONNECTION_STRING="postgresql://user:pass@host:5439/database"')
    exit(1)

print("Intentando conectar a Redshift...")
print(f"Connection string (ofuscada): postgresql://***:***@{conn_str.split('@')[1] if '@' in conn_str else '???'}")

try:
    # Intentar conexión
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()
    
    # Ejecutar query simple
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    
    print("\n✓ CONEXIÓN EXITOSA!")
    print(f"✓ Versión de Redshift: {version[0][:80]}...")
    
    # Listar bases de datos
    cursor.execute("""
        SELECT datname 
        FROM pg_database 
        WHERE datistemplate = false;
    """)
    databases = cursor.fetchall()
    
    print(f"\n✓ Bases de datos disponibles:")
    for db in databases:
        print(f"  - {db[0]}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*70)
    print("TODO OK - Podés continuar con el Paso 2")
    print("="*70)

except Exception as e:
    print("\n✗ ERROR AL CONECTAR")
    print("="*70)
    print(f"Error: {str(e)}")
    print("="*70)
    print("\nPosibles causas:")
    print("1. Connection string incorrecta en .env")
    print("2. Cluster apagado o pausado")
    print("3. Security Group no permite tu IP")
    print("4. Credenciales incorrectas")
    exit(1)