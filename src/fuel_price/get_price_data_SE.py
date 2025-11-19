# Obtener datos de precios de combustibles de la Secretaria de Energia de la Nacion (Argentina)

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


def check_historical_data_exists(data_path):
    """
    Verifica si los datos historicos (2022-2024) ya estan descargados y son validos.

    Args:
        data_path: Directorio donde se guardan los datos

    Returns:
        True si los datos historicos existen y son validos (podemos usar cache)
        False si hay que descargarlos de nuevo
    """
    print("\nVerificando datos historicos en cache...")
    print("-" * 70)

    # Buscar archivos CSV historicos usando patron de nombre
    csv_files = list(data_path.glob("*2022_2024*public*.csv"))

    if not csv_files:
        # No encontramos ningun archivo, hay que descargar
        print("No se encontraron datos historicos (2022-2024) en cache")
        print("Se descargaran datos completos")
        print("-" * 70)
        return False

    # Tomamos el primer archivo encontrado (normalmente hay solo uno)
    csv_file = csv_files[0]
    print(f"Archivo encontrado: {csv_file.name}")

    # Validar que el archivo tenga contenido (no este vacio)
    try:
        file_size = csv_file.stat().st_size

        if file_size == 0:
            print("Archivo existe pero esta vacio")
            print("Se redescargaran los datos historicos")
            print("-" * 70)
            return False

        # Intentar leer las primeras 5 lineas para verificar que es un CSV valido
        df = pd.read_csv(csv_file, nrows=5)

        if len(df) == 0:
            print("Archivo no contiene datos")
            print("Se redescargaran los datos historicos")
            print("-" * 70)
            return False

        print("\nCache valido. Se omitira descarga de datos historicos")
        print("-" * 70)
        return True

    except Exception as e:
        print(f"Error al validar archivo: {e}")
        print("Se redescargaran los datos historicos por seguridad")
        print("-" * 70)
        return False


def download_and_extract_access_files(
    urls, data_path, force_download=False, force_labels=None
):
    """
    Descarga archivos ZIP y extrae archivos .accdb con sistema de cache inteligente.

    El sistema valida que realmente existan los archivos .accdb antes de confiar
    en el marcador .done, para auto-repararse en caso de inconsistencias.

    Args:
        urls: Diccionario {label: url} de archivos a descargar
        data_path: Directorio donde guardar los datos
        force_download: Si True, ignora cache y descarga todo de nuevo
        force_labels: Iterable de etiquetas que deben descargarse y extraerse siempre

    Returns:
        Lista de rutas a archivos .accdb extraidos
    """
    force_labels = set(force_labels or [])
    data_path.mkdir(parents=True, exist_ok=True)
    access_files = []

    for label, url in urls.items():
        print(f"\n{'='*70}")
        print(f"PROCESANDO: {label}")
        print("=" * 70)

        zip_name = f"{label}.zip"
        zip_path = data_path / zip_name
        extracted_marker = data_path / f"{label}.done"
        force_this = force_download or (label in force_labels)

        # Verificar si ya existe el archivo ZIP descargado
        if zip_path.exists() and not force_this:
            size_mb = zip_path.stat().st_size / (1024 * 1024)
            print(f"ZIP existente encontrado: {zip_name} ({size_mb:.2f} MB)")
            print("Omitiendo descarga (usando cache)")
        else:
            print(f"Descargando desde: {url}")
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                with open(zip_path, "wb") as f:
                    f.write(response.content)
                size_mb = zip_path.stat().st_size / (1024 * 1024)
                print(f"Descarga completada: {zip_name} ({size_mb:.2f} MB)")
            except requests.RequestException as e:
                print(f"ERROR al descargar: {e}")
                print(f"Saltando {label}")
                continue

        # Verificar si ya se extrajo el ZIP anteriormente
        if extracted_marker.exists() and not force_this:
            print(f"\nMarcador de extraccion encontrado: {label}.done")
            found_accdb = list(data_path.glob(f"*{label}*.accdb"))
            if found_accdb:
                print(f"Archivos .accdb verificados: {len(found_accdb)} encontrados")
                for accdb in found_accdb:
                    print(f"  - {accdb.name}")
                print("Omitiendo extraccion (usando cache)")
                access_files.extend(found_accdb)
                continue
            else:
                print("\nMarcador existe pero no se encuentran archivos .accdb")
                print(
                    "Posible causa: archivos borrados o extraccion interrumpida previamente"
                )
                print(f"Eliminando marcador corrupto: {label}.done")
                extracted_marker.unlink()
                print("Procediendo a re-extraer el archivo ZIP...")

        print(f"\nExtrayendo {zip_name}...")
        try:
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(data_path)
                for filename in z.namelist():
                    if filename.endswith(".accdb"):
                        accdb_path = data_path / filename
                        access_files.append(accdb_path)
                        print(f"  Extraido: {filename}")
            extracted_marker.touch()
            print(f"\nExtraccion completada exitosamente")
            print(f"Marcador creado: {label}.done")
        except zipfile.BadZipFile as e:
            print(f"Archivo ZIP corrupto - {e}")
            print(f"Intenta eliminar {zip_path} y volver a ejecutar")
            continue

    print(f"\n{'='*70}")
    print(f"RESUMEN DE DESCARGA Y EXTRACCION")
    print("=" * 70)
    print(f"Total de archivos Access (.accdb) disponibles: {len(access_files)}")
    for accdb in access_files:
        print(f"  - {accdb.name}")

    return access_files


def get_access_tables(accdb_path):
    """Obtiene la lista de tablas de un archivo Access usando mdbtools"""

    try:
        result = subprocess.run(
            ["mdb-tables", "-1", str(accdb_path)],
            capture_output=True,
            text=True,
            check=True,
        )

        tables = [
            line.strip() for line in result.stdout.strip().split("\n") if line.strip()
        ]

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
        True si tuvo exito, False si fallo
    """
    try:
        # Ejecutar mdb-export
        result = subprocess.run(
            ["mdb-export", str(accdb_path), table_name],
            capture_output=True,
            text=True,
            check=True,
        )

        # Guardar resultado en CSV
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result.stdout)

        # Verificar que se creo el archivo
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
    print(f"\nProcesando: {db_file_path.name}")
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


def concatenate_csv_files(csv_files, data_path):
    """
    Concatena archivos CSV de precios, manejando datos existentes y eliminando duplicados.

    Args:
        csv_files: Lista de archivos CSV recien generados en esta ejecucion
        data_path: Directorio donde guardar el archivo concatenado

    Returns:
        Path al archivo concatenado o None si fallo
    """
    print("\n" + "=" * 70)
    print("CONCATENANDO ARCHIVOS CSV")
    print("=" * 70)

    # Filtrar solo los archivos que contienen datos de precios
    price_files = [f for f in csv_files if "public" in f.name.lower()]

    output_file = data_path / "precios_eess_completo.csv"

    # Caso especial: No hay archivos nuevos esta vez
    if not price_files:
        print("\nNo hay archivos nuevos para procesar en esta ejecucion")

        # Verificar si ya existe un archivo completo de ejecuciones anteriores
        if output_file.exists():
            size_mb = output_file.stat().st_size / (1024 * 1024)
            print(f"Usando archivo completo existente: {output_file.name}")
            print(f"Tamaño: {size_mb:.2f} MB")
            return output_file
        else:
            print("No hay datos nuevos ni archivo completo previo")
            return None

    print(f"\nArchivos nuevos a procesar: {len(price_files)}")
    for f in price_files:
        size_kb = f.stat().st_size / 1024
        print(f"  - {f.name} ({size_kb:.2f} KB)")

    try:
        # Leer todos los archivos nuevos y concatenarlos
        print("\nLeyendo archivos nuevos...")
        new_frames = []
        total_new_records = 0

        for csv_file in price_files:
            df = pd.read_csv(csv_file)
            print(f"  {csv_file.name}: {len(df):,} registros")
            new_frames.append(df)
            total_new_records += len(df)

        new_df = pd.concat(new_frames, ignore_index=True)
        print(f"\nTotal de registros nuevos: {total_new_records:,}")

        # Verificar si ya existe un archivo completo de ejecuciones anteriores
        if output_file.exists():
            print(f"\nArchivo completo existente encontrado: {output_file.name}")

            # Leer el archivo existente
            existing_df = pd.read_csv(output_file)
            print(f"Registros existentes: {len(existing_df):,}")

            # Combinar datos existentes + nuevos
            print("\nCombinando datos existentes con nuevos...")
            combined = pd.concat([existing_df, new_df], ignore_index=True)

            # Eliminar duplicados
            records_before = len(combined)
            combined = combined.drop_duplicates().reset_index(drop=True)
            records_after = len(combined)
            duplicates_removed = records_before - records_after

            print(f"Registros despues de combinar: {records_before:,}")
            print(f"Duplicados eliminados: {duplicates_removed:,}")
            print(f"Registros finales: {records_after:,}")

        else:
            # No existe archivo previo, esta es la primera ejecucion
            print("\nPrimera ejecucion - creando archivo completo desde cero")
            combined = new_df

        # Guardar el archivo completo actualizado
        print(f"\nGuardando archivo completo...")
        combined.to_csv(output_file, index=False)

        size_mb = output_file.stat().st_size / (1024 * 1024)

        print(f"\n{'='*70}")
        print("ARCHIVO COMPLETO ACTUALIZADO EXITOSAMENTE")
        print("=" * 70)
        print(f"Nombre: {output_file.name}")
        print(f"Registros totales: {len(combined):,}")
        print(f"Columnas: {len(combined.columns)}")
        print(f"Tamaño: {size_mb:.2f} MB")
        print(f"Ubicacion: {output_file}")

        return output_file

    except Exception as e:
        print(f"\nERROR al concatenar archivos: {e}")
        return None


def main():
    """
    Función principal con sistema de cache inteligente.
    Esta funcion implementa un sistema de cache inteligente que decide automaticamente
    que datos descargar segun lo que ya este disponible en disco.
    """
    print("\n" + "=" * 70)
    print("EXTRACTOR DE DATOS DE COMBUSTIBLES - SECRETARIA DE ENERGIA")
    print("Sistema de cache automatico - Version optimizada")
    print("=" * 70)

    # Definir las URLs de las fuentes de datos
    # STATIC: Datos historicos que no cambian (2022-2024)
    # INCREMENTAL: Datos actuales que se actualizan continuamente (2025+)
    STATIC_URL = "http://res1104.se.gob.ar/adjuntos/precios_eess_2022_2024.zip"
    INCREMENTAL_URL = (
        "http://res1104.se.gob.ar/adjuntos/precios_eess_2025_en_adelante.zip"
    )

    # Determinar directorio de trabajo
    project_root = Path(__file__).parent.parent.parent
    data_path = project_root / "data" / "raw"

    print(f"\nDirectorio de salida: {data_path}")
    print(f"Directorio completo: {data_path.absolute()}")

    # 1. Verificar que mdbtools este instalado
    print("\n" + "=" * 70)
    print("VERIFICANDO DEPENDENCIAS")
    print("=" * 70)

    if check_mdbtools_installed():
        print("mdbtools: INSTALADO")
    else:
        print("\nERROR: mdbtools no esta instalado")
        print("\nPara instalar:")
        print("  Ubuntu/Debian: sudo apt-get install mdbtools")
        print("  macOS: brew install mdbtools")
        return 1

    # 2. Verificar datos historicos en cache
    print("\n" + "=" * 70)
    print("DECISION AUTOMATICA DE DESCARGA")
    print("=" * 70)

    historical_exists = check_historical_data_exists(data_path)

    # Construir diccionario de URLs a descargar segun la decision
    urls = {}

    if historical_exists:
        print("\nMODO: ACTUALIZACION INCREMENTAL")
        print("Datos historicos (2022-2024): CACHE (no se descargaran)")
        print("Datos actuales (2025+): SE DESCARGARAN")

        # Solo agregar URL de datos actuales
        urls["2025_plus"] = INCREMENTAL_URL

    else:
        print("\nMODO: DESCARGA COMPLETA")
        print("Datos historicos (2022-2024): SE DESCARGARAN")
        print("Datos actuales (2025+): SE DESCARGARAN")
        print("\nEsta es probablemente la primera ejecucion o los datos previos")
        print("no son validos. Se descargaran datos completos.")

        # Agregar ambas URLs
        urls["2022_2024"] = STATIC_URL
        urls["2025_plus"] = INCREMENTAL_URL

    # 3. Descargar y extraer archivos segun decision
    print("\n" + "=" * 70)
    print("DESCARGA Y EXTRACCION DE DATOS")
    print("=" * 70)

    access_files = download_and_extract_access_files(
        urls, data_path, force_labels={"2025_plus"}
    )

    if not access_files:
        print(
            "\nERROR: No se encontraron archivos Access despues de descarga/extraccion"
        )
        print("Verifica tu conexion a internet y que las URLs sean validas")
        return 1

    # 4. Procesar archivos Access (convertir .accdb a .csv)
    print("\n" + "=" * 70)
    print("PROCESANDO BASES DE DATOS ACCESS")
    print("=" * 70)

    all_csv_files = []
    for accdb_file in access_files:
        csv_files = process_access_db_with_mdbtools(accdb_file, data_path)
        all_csv_files.extend(csv_files)

    print(f"\nTotal de archivos CSV generados: {len(all_csv_files)}")

    # 5. Concatenar todos los CSV en un archivo completo
    concatenated_file = concatenate_csv_files(all_csv_files, data_path)

    # 6. Resumen final
    print("\n" + "=" * 70)
    print("PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 70)

    if concatenated_file:
        print(f"\nArchivo final generado: {concatenated_file.name}")
        print(f"Ubicacion completa: {concatenated_file.absolute()}")
        print("\nLos datos estan listos para ser utilizados en el pipeline ETL")

        # Dar indicacion de como usar el archivo
        print("\nPara usar estos datos en tu pipeline:")
        print("  import pandas as pd")
        print(f"  df = pd.read_csv('{concatenated_file}')")

    else:
        print("\nADVERTENCIA: No se genero archivo completo")
        print("Revisa los mensajes de error anteriores")

    print("\n" + "=" * 70 + "\n")

    return 0


if __name__ == "__main__":
    # Ejecutar la funcion principal
    # No necesita parametros - todo es automatico
    exit_code = main()
    sys.exit(exit_code)
