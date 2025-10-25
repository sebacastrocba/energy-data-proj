# Obtener datos de precios de combustibles de la Secretaria de Energía de la Nación (Argentina) en Linux

import pandas as pd
import os
from pathlib import Path
import requests
import zipfile
import io
import sys 
import subprocess
import shutil

def check_mdbtools_installed():
    """Verifica si mdbtools esta instalado en el sistema."""
    return shutil.which("mdb-export") is not None

def download_and_extract_access_files(urls, data_path):
    """Descarga archivos ZIP y extrae archivos .accdb."""

    data_path.mkdir(parents=True, exist_ok=True)
    access_files = []

    for url in urls:
        print(f"\nDescargando y extrayendo desde: {url}")
        
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:

                for filename in z.namelist():
                    extracted_path = data_path / filename 

                    z.extract(filename, data_path)

                    if filename.endswith('.accdb'):
                        access_files.append(extracted_path)

        except requests.RequestException as e:
            print(f"Error descargando {url}: {e}")
            continue

        except zipfile.BadZipFile as e:
            print(f"Error: archivo ZIP corrupto de {url}: {e}")
            continue

    print(f"\nDescarga completa. Total de archivos Access encontrados: {len(access_files)}")
    return access_files
    
def get_access_tables(accdb_path):
    """Obtiene la lista de tablas de un archivo Access usando mdbtools"""

    try:
        result = subprocess.run(
            ["mdb-tables", '-1', str(accdb_path)],
            capture_output=True, text=True, check=True
        )

        tables = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]

        return tables
    
    except subprocess.CalledProcessError as e:
        print(f"Error obteniendo tablas de {accdb_path}: {e}")
        return []

def export_access_table_to_csv(accdb_path, table_name, output_path):
    """
    Exporta una tabla de Access a CSV usando mdbtools
    
    Args:
        accdb_path: Ruta al archivo .accdb
        table_name: Nombre de la tabla a exportar
        output_path: Ruta donde guardar el CSV
        
    Returns:
        True si tuvo éxito, False si falló
    """
    try:
        # Ejecutar mdb-export
        result = subprocess.run(
            ['mdb-export', str(accdb_path), table_name],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Guardar resultado en CSV
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result.stdout)
        
        # Verificar que se creó el archivo
        if output_path.exists() and output_path.stat().st_size > 0:
            return True
        else:
            return False
    
    except subprocess.CalledProcessError as e:
        print(f"Error exportando tabla '{table_name}': {e}")
        return False

def process_access_db_with_mdbtools(db_file_path, data_path):
    """
    Procesa un archivo Access y exporta todas sus tablas a CSV usando mdbtools
    
    Args:
        db_file_path: Ruta al archivo .accdb
        data_path: Directorio donde guardar los CSV
        
    Returns:
        Lista de archivos CSV creados
    """
    print(f"Procesando: {db_file_path.name}")
    print("-" * 70)
    
    # Obtener tablas
    print("Listando tablas...")
    tables = get_access_tables(db_file_path)
    
    if not tables:
        print("No se encontraron tablas")
        return []

    print(f"Encontradas {len(tables)} tablas: {', '.join(tables)}")

    # Exportar cada tabla
    csv_files = []
    for table in tables:
        print(f"\nExportando tabla: '{table}'...")
        
        # Nombre del archivo CSV
        csv_filename = f"{db_file_path.stem}_{table}.csv"
        csv_path = data_path / csv_filename
        
        # Exportar
        success = export_access_table_to_csv(db_file_path, table, csv_path)
        
        if success:
            # Leer con pandas para validar y mostrar info
            try:
                df = pd.read_csv(csv_path)
                print(f"Exportado: {csv_filename}")
                print(f"Registros: {len(df):,}")
                print(f"Columnas: {len(df.columns)}")
                print(f"Tamaño: {csv_path.stat().st_size / 1024:.2f} KB")
                csv_files.append(csv_path)
            except Exception as e:
                print(f"Advertencia al leer CSV: {e}")
        else:
            print(f"Error exportando '{table}'")
    
    print("-" * 70)
    return csv_files

def main():
    """
    Función principal
    """
    print("\n" + "="*70)
    print("EXTRACTOR DE DATOS DE COMBUSTIBLES - SECRETARÍA DE ENERGÍA")
    print("="*70)
    
    # URLs de los datos
    urls = [
        "http://res1104.se.gob.ar/adjuntos/precios_eess_2022_2024.zip",
        "http://res1104.se.gob.ar/adjuntos/precios_eess_2025_en_adelante.zip"
    ]
    
    # Directorio de salida
    project_root = Path(__file__).parent.parent.parent
    data_path = project_root / "data" / "raw"
    
    print(f"Directorio de salida: {data_path}")
    
    # 1. Verificar mdbtools
    print("Verificando dependencias...")
    if check_mdbtools_installed():
        print("mdbtools instalado correctamente.")
    else:
        print("ERROR: mdbtools no está instalado.")
    
    # 2. Descargar y extraer archivos
    access_files = download_and_extract_access_files(urls, data_path)
    
    if not access_files:
        print("ERROR: No se pudieron descargar archivos Access")
        return 1
    
    # 3. Procesar cada archivo Access
    print("\n" + "="*70)
    print("PROCESANDO BASES DE DATOS ACCESS")
    print("="*70)
    
    all_csv_files = []
    for accdb_file in access_files:
        csv_files = process_access_db_with_mdbtools(accdb_file, data_path)
        all_csv_files.extend(csv_files)
    
    # 4. Resumen final
    print("\n" + "="*70)
    print("PROCESO COMPLETADO")
    print("="*70)
    print(f"\nArchivos CSV creados: {len(all_csv_files)}")
    
    if all_csv_files:
        print("\nArchivos generados:")
        for csv_file in all_csv_files:
            size_kb = csv_file.stat().st_size / 1024
            print(f"   • {csv_file.name} ({size_kb:.2f} KB)")

        print(f"\nUbicación: {data_path}")
        print("\nDatos listos para usar")
    else:
        print("No se generaron archivos CSV")
        print("Verifica los errores anteriores")
    
    print("\n" + "="*70 + "\n")
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
