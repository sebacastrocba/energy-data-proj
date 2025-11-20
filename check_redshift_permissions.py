"""Verificar qué schemas existen y qué permisos tenemos"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn_str = os.getenv("REDSHIFT_CONNECTION_STRING")

conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

print("SCHEMAS EXISTENTES:")
print("=" * 70)
cursor.execute("""
    SELECT nspname 
    FROM pg_namespace 
    WHERE nspname NOT LIKE 'pg_%' 
    AND nspname != 'information_schema'
    ORDER BY nspname;
""")
schemas = cursor.fetchall()
for schema in schemas:
    print(f"  - {schema[0]}")

print("\n¿PUEDO CREAR TABLAS EN public?")
print("=" * 70)
cursor.execute("""
    SELECT has_schema_privilege('public', 'CREATE') as can_create;
""")
result = cursor.fetchone()
print(f"  CREATE en public: {result[0]}")

cursor.close()
conn.close()