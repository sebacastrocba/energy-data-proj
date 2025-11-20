# Análisis de Precios de Combustibles en Argentina

![CI](https://github.com/sebacastrocba/energy-data-proj/workflows/CI/badge.svg)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Descripción del Proyecto

Este proyecto implementa un pipeline de datos (ETL) completo que extrae, transforma y carga información sobre precios de combustibles en Argentina, combinándola con datos del precio internacional del petróleo Brent y cotizaciones USD/ARS (oficial y blue). El objetivo es analizar la correlación entre los precios locales, el mercado internacional de petróleo y la evolución del tipo de cambio.

**Caso de uso:** Permite a analistas y tomadores de decisiones entender cómo fluctúan los precios de combustibles en Argentina en relación con el precio del petróleo Brent y el tipo de cambio, identificando patrones temporales y geográficos.

**⚠️ Nota sobre disponibilidad de datos:** Los datos de combustibles de la Secretaría de Energía están disponibles únicamente desde 2025 en adelante, debido a que las tablas correspondientes al período 2022-2024 no se encuentran disponibles en la fuente oficial al momento del desarrollo del proyecto.

## Características Principales

- ✅ **Extracción de datos** de múltiples fuentes (Secretaría de Energía, Yahoo Finance, Bluelytics API)
- ✅ **Transformación y limpieza** de datos con pandas y numpy
- ✅ **Carga a PostgreSQL** con Docker Compose (schemas staging y analytics)
- ✅ **Carga a AWS Redshift** para producción (opcional)
- ✅ **Orquestación con Apache Airflow** con carga paralela a múltiples destinos
- ✅ **Pipeline ETL automatizado** con ejecución diaria programada
- ✅ **Análisis exploratorio** con Jupyter notebooks
- ✅ **Tests automatizados** con pytest y pytest-cov
- ✅ **Type checking** con mypy
- ✅ **Code quality** con black y flake8
- ✅ **Gestión de dependencias** con Poetry
- ✅ **Context managers** para manejo seguro de conexiones DB

## 🚀 Inicio Rápido

### Requisitos Previos

- Docker y Docker Compose instalados
- Python 3.12+
- Poetry (recomendado) o pip

### Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/sebacastrocba/energy-data-proj.git
cd fuel_price_project

# 2. Crear archivo de configuración
cp .env.example .env
# Edita .env si necesitas cambiar credenciales

# 3. Levantar PostgreSQL con Docker
docker-compose up -d

# 4. Instalar dependencias Python con Poetry
poetry install
```

El archivo `sql/init.sql` se ejecuta automáticamente al crear el contenedor de PostgreSQL por primera vez, inicializando los schemas y tablas necesarias.

## 📊 Uso del Pipeline

### Pipeline ETL Paso a Paso

El pipeline ETL se ejecuta en tres pasos secuenciales:

**Paso 1: Extracción** - Descarga datos de APIs y los guarda en `data/raw/` como CSV
```bash
poetry run python src/fuel_price/extract.py
```
- Descarga precios de Brent desde Yahoo Finance
- Descarga cotizaciones USD/ARS desde Bluelytics API
- Extrae datos de combustibles de la Secretaría de Energía
- Guarda archivos CSV en `data/raw/`

**Paso 2: Transformación** - Lee CSVs, limpia y agrega datos, genera Parquets en `data/processed/`
```bash
poetry run python src/fuel_price/transform.py
```
- Lee archivos CSV de `data/raw/`
- Limpia y valida datos
- Genera agregaciones mensuales
- Guarda archivos Parquet en `data/processed/`

**Paso 3: Carga** - Lee Parquets y carga a PostgreSQL
```bash
poetry run python src/fuel_price/load.py
```
- Lee archivos Parquet de `data/processed/`
- Carga datos a tablas `staging.*` (datos limpios)
- Carga agregaciones a tablas `analytics.*` (datos mensuales)

### Usar como Librería

```python
from fuel_price.extract import extract_brent_prices, extract_dolar_bluelytics, extract_fuel_prices
from fuel_price.transform import (
    clean_brent_price, 
    agg_brent_price_for_analytics,
    clean_fuel_price, 
    agg_fuel_price_for_analytics,
    clean_dollar_price,
    agg_dollar_price_for_analytics
)
from fuel_price.load import (
    load_brent_to_staging, 
    load_brent_to_analytics,
    load_fuel_to_staging,
    load_fuel_to_analytics,
    load_dollar_to_staging,
    load_dollar_to_analytics
)

# Extraer
brent_raw = extract_brent_prices()
fuel_raw = extract_fuel_prices()
usd_raw = extract_dolar_bluelytics()

# Transformar
brent_clean = clean_brent_price(brent_raw)
brent_analytics = agg_brent_price_for_analytics(brent_clean)

# Cargar a PostgreSQL
load_brent_to_staging(brent_clean, truncate=True)
load_brent_to_analytics(brent_analytics, truncate=True)
```


## 🗂️ Estructura de Base de Datos

### Schemas Creados

- **`staging`**: Datos crudos o mínimamente procesados
- **`analytics`**: Datos transformados y agregados a nivel mensual

### Tablas Staging

| Tabla | Descripción | Campos Principales |
|-------|-------------|-------------------|
| `staging.brent_price` | Precios diarios del petróleo Brent | `date`, `brent_price` |
| `staging.fuel_prices` | Precios de combustibles por estación | `periodo`, `provincia`, `producto`, `precio_surtidor`, `volumen` |
| `staging.usd_ars_rates` | Cotización USD/ARS (oficial y blue) | `date`, `source`, `value_buy`, `value_sell` |

### Tablas Analytics (Agregaciones Mensuales)

| Tabla | Descripción | Agregaciones |
|-------|-------------|--------------|
| `analytics.brent_price_monthly` | Brent agregado mensualmente | Promedio, min, max, desviación estándar |
| `analytics.fuel_prices_monthly` | Combustibles por mes/provincia/producto | Promedio ponderado por volumen |
| `analytics.usd_ars_rates_monthly` | USD/ARS mensual con brecha cambiaria | Promedio blue, oficial, brecha % |

## 🔍 Explorar la Base de Datos

### Conectar con psql

```bash
# Usando las credenciales por defecto
PGPASSWORD=fuel_password psql -h localhost -p 5432 -U fuel_user -d fuel_prices_db
```

### Consultas Útiles

```sql
-- Ver schemas
\dn

-- Ver tablas de staging
\dt staging.*

-- Ver tablas de analytics
\dt analytics.*

-- Ver estructura de una tabla
\d staging.brent_price

-- Consultar últimos precios de Brent
SELECT * FROM staging.brent_price ORDER BY date DESC LIMIT 10;

-- Ver agregación mensual de combustibles por provincia
SELECT year, month, provincia, producto, avg_price 
FROM analytics.fuel_prices_monthly 
WHERE year = 2024 AND provincia = 'BUENOS AIRES'
ORDER BY month DESC, producto
LIMIT 20;

-- Comparar brecha cambiaria mensual
SELECT year, month, avg_blue, avg_oficial, brecha_pct
FROM analytics.usd_ars_rates_monthly
ORDER BY year DESC, month DESC
LIMIT 12;
```

## 🐳 Comandos Docker Útiles

```bash
# Ver estado del contenedor
docker-compose ps

# Ver logs
docker-compose logs -f

# Detener (datos se mantienen)
docker-compose stop

# Reiniciar
docker-compose restart

# Eliminar todo (BORRA DATOS)
docker-compose down -v
```

## 🧪 Testing

El proyecto incluye tests automatizados para las funciones de extracción y transformación.

### Ejecutar todos los tests

```bash
poetry run pytest
```

### Ejecutar tests con cobertura

```bash
poetry run pytest --cov=src/fuel_price --cov-report=html
```

El reporte HTML se genera en `htmlcov/index.html`.

### Ejecutar tests específicos

```bash
# Solo tests de extracción
poetry run pytest tests/test_extract.py

# Solo tests de transformación
poetry run pytest tests/test_transform.py

# Ejecutar un test específico
poetry run pytest tests/test_transform.py::test_agg_brent_price_calculates_monthly_average
```

### Verificación de tipos con mypy

```bash
poetry run mypy src/fuel_price
```

## 📁 Estructura del Proyecto

```
fuel_price_project/
├── src/
│   └── fuel_price/
│       ├── __init__.py
│       ├── config.py            # Configuración global y constantes
│       ├── extract.py           # Extracción de datos de APIs
│       ├── transform.py         # Transformación y limpieza de datos
│       ├── load.py              # Carga a PostgreSQL con context managers
│       ├── load_redshift.py     # Carga a AWS Redshift
│       └── get_price_data_SE.py # Extractor de datos de Secretaría de Energía
├── tests/
│   ├── __init__.py
│   ├── test_extract.py          # Tests de extracción
│   └── test_transform.py        # Tests de transformación
├── dags/
│   └── fuel_price_dag.py        # DAG de Airflow con carga paralela
├── notebooks/
│   └── 00_analisis_exploratorio.ipynb  # Análisis exploratorio de datos
├── sql/
│   └── init.sql                 # Schema SQL (staging + analytics)
├── data/
│   ├── raw/                     # Datos originales (gitignored)
│   │   ├── 2025_plus.zip        # Archivo descargado de SE (2025+)
│   │   ├── 2025_plus.done       # Marca de descarga completada
│   │   ├── brent_prices.csv     # Precios históricos de Brent
│   │   ├── usd_ars_bluelytics.csv  # Cotizaciones USD/ARS
│   │   ├── precios_eess_completo.csv  # Datos consolidados de combustibles
│   │   ├── precios_eess_2025_en_adelante.accdb  # Base Access original SE
│   │   └── precios_eess_2025_en_adelante_public_vi_access_eess_2025_en_adelante.csv
│   └── processed/               # Datos procesados (gitignored)
│       ├── brent_price_cleaned.parquet    # Precios Brent limpios
│       ├── brent_price_monthly.parquet    # Agregación mensual Brent
│       ├── fuel_price_cleaned.parquet     # Precios combustibles limpios
│       ├── fuel_price_aggregated.parquet  # Agregación mensual combustibles
│       ├── dollar_price_cleaned.parquet   # Cotizaciones limpias
│       └── dollar_price_aggregated.parquet  # Agregación mensual USD/ARS
├── logs/                        # Logs de Airflow (gitignored)
│   ├── scheduler/               # Logs del scheduler
│   └── dag_id=fuel_price_etl/   # Logs por ejecución del DAG
├── docs/                        # Documentación adicional
├── docker-compose.yml           # Configuración de PostgreSQL y Airflow
├── Dockerfile.airflow           # Dockerfile para Airflow con dependencias
├── airflow.cfg                  # Configuración de Airflow
├── airflow.db                   # Base de datos SQLite (legacy, no usado)
├── generate_fernet_key.py       # Script para generar FERNET_KEY
├── pyproject.toml               # Configuración de Poetry y dependencias
├── poetry.lock                  # Lock file de Poetry
├── mypy.ini                     # Configuración de mypy
├── .flake8                      # Configuración de flake8
├── .env.example                 # Plantilla de variables de entorno
├── .env                         # Variables de entorno (gitignored)
└── README.md                    # Este archivo
```

## 📚 Fuentes de Datos

- **Precios de Combustibles:** [Secretaría de Energía de Argentina](http://res1104.se.gob.ar/) - Datos desde 2025 en adelante
  - ⚠️ **Nota:** Los datos históricos 2022-2024 no están disponibles en la fuente oficial al momento del desarrollo
  - Se utilizan únicamente datos desde 2025 debido a la indisponibilidad de tablas anteriores
- **Precio del Brent:** [Yahoo Finance](https://finance.yahoo.com/) (símbolo: BZ=F) - Precios diarios
- **Tipo de Cambio USD/ARS:** [Bluelytics API](https://bluelytics.com.ar/#!/api) - Cotizaciones oficial y blue

## 🔧 Tecnologías Utilizadas

- **Lenguaje:** Python 3.12
- **Gestión de dependencias:** Poetry
- **Base de datos:** PostgreSQL 15 (Alpine)
- **Data Warehouse:** AWS Redshift (opcional)
- **Contenedores:** Docker y Docker Compose
- **Orquestación:** Apache Airflow 2.10.3 (LocalExecutor)
- **Análisis de datos:** pandas, numpy
- **Testing:** pytest, pytest-cov, pytest-mock
- **Type checking:** mypy
- **Code formatting:** black, flake8
- **Formato de datos:** Parquet (PyArrow)
- **Conexión DB:** psycopg2 (PostgreSQL y Redshift)

## 🛠️ Solución de Problemas

### PostgreSQL no inicia

```bash
# Ver logs del contenedor
docker-compose logs postgres

# Verificar que el contenedor esté corriendo
docker-compose ps

# Reiniciar contenedor
docker-compose restart postgres

# Reiniciar desde cero (ELIMINA TODOS LOS DATOS)
docker-compose down -v
docker-compose up -d
```

### Error de conexión a PostgreSQL

```bash
# Verificar que el contenedor esté corriendo
docker-compose ps

# Probar conexión desde Python
poetry run python -c "from src.fuel_price.load import test_connection; print('OK' if test_connection() else 'ERROR')"

# Verificar variables de entorno
cat .env
```

### Problemas con dependencias de Python

```bash
# Reinstalar dependencias
poetry install --no-cache

# Actualizar Poetry
poetry self update

# Verificar versión de Python
python --version  # Debe ser 3.12+
```

### Error al extraer datos de la Secretaría de Energía

Los archivos `.accdb` requieren procesamiento especial. El script `get_price_data_SE.py` maneja la conversión automáticamente.

```bash
# Ejecutar extractor manualmente
poetry run python src/fuel_price/get_price_data_SE.py
```

### Problemas con Airflow

**Airflow no inicia:**
```bash
# Ver logs del scheduler
docker logs airflow_scheduler

# Ver logs del webserver
docker logs airflow_webserver

# Verificar que la base de datos de Airflow esté inicializada
docker exec airflow_scheduler airflow db check

# Reiniciar contenedores de Airflow
docker-compose restart airflow_scheduler airflow_webserver
```

**DAG no aparece en la UI:**
```bash
# Verificar que el archivo DAG esté en el directorio correcto
ls -la dags/

# Verificar que no haya errores de sintaxis
poetry run python dags/fuel_price_dag.py

# Verificar desde Airflow
docker exec airflow_webserver airflow dags list | grep fuel

# Forzar actualización de DAGs
docker exec airflow_scheduler airflow dags reserialize
```

**Error en tarea load_redshift:**
```bash
# Verificar que las credenciales estén configuradas
cat .env | grep REDSHIFT

# Probar conexión a Redshift
poetry run python -c "from fuel_price.load_redshift import test_redshift_connection; test_redshift_connection()"

# Ver logs específicos de la tarea
docker exec airflow_scheduler airflow tasks logs fuel_price_etl load_redshift <RUN_ID>
```

**DAG atascado o en estado indefinido:**
```bash
# Marcar tarea como fallida manualmente
docker exec airflow_scheduler airflow tasks clear fuel_price_etl -t <task_id> -s <start_date> -e <end_date>

# Reiniciar scheduler
docker-compose restart airflow_scheduler
```

**Problemas de permisos:**
```bash
# Verificar permisos de directorios
ls -la logs/ dags/

# Ajustar permisos si es necesario
chmod -R 755 logs/ dags/

# Verificar usuario de los contenedores
docker exec airflow_scheduler whoami
```

## 📝 Notas Importantes

- **Puerto:** PostgreSQL corre en el puerto **5432** (puerto estándar)
- **Credenciales:** Las credenciales por defecto son:
  - Usuario: `fuel_user`
  - Contraseña: `fuel_password`
  - Base de datos: `fuel_prices_db`
- **Variables de entorno:** Configura el archivo `.env` para personalizar las credenciales
- **Volúmenes:** Los datos de PostgreSQL persisten en un volumen Docker llamado `postgres_data`
- **Schemas:** 
  - `staging`: Para datos crudos o mínimamente procesados
  - `analytics`: Para datos agregados y transformados
- **Inicialización automática:** El archivo `sql/init.sql` se ejecuta automáticamente al crear el contenedor por primera vez
- **Reinicializar:** Para borrar todos los datos y empezar de cero: `docker-compose down -v && docker-compose up -d`
- **Datos:** Los archivos CSV en `data/raw/` están en `.gitignore` y no se suben al repositorio
- **Context managers:** El código usa context managers para manejar conexiones a la base de datos de forma segura
- **⚠️ Limitación de datos:** Los datos de combustibles de la Secretaría de Energía están disponibles únicamente desde 2025, ya que las tablas correspondientes a 2022-2024 no están disponibles en la fuente oficial

## 🤖 Orquestación con Apache Airflow

### Configuración Inicial de Airflow

Airflow se ejecuta mediante Docker Compose. Para iniciar los servicios de Airflow:

```bash
# Iniciar todos los servicios (PostgreSQL + Airflow)
docker-compose up -d

# Verificar que los servicios estén corriendo
docker-compose ps
```

**Acceso a la UI:**
- URL: http://localhost:8080
- Usuario: `airflow`
- Contraseña: `airflow`

**Nota:** Las credenciales por defecto pueden configurarse en el archivo `docker-compose.yml`.

### Configuración de Airflow

Airflow está configurado con:
- **Base de datos:** PostgreSQL dedicada para metadatos de Airflow (`airflow_postgres`)
- **Ejecutor:** LocalExecutor (permite paralelismo)
- **DAGs folder:** `./dags`
- **Logs folder:** `./logs`
- **Ejemplos:** Desactivados (`load_examples = False`)

**Componentes en Docker:**
- `airflow_webserver` - Interfaz web (puerto 8080)
- `airflow_scheduler` - Programador de tareas
- `airflow_init` - Inicialización de la base de datos (se ejecuta una vez)
- `postgres-airflow` - Base de datos PostgreSQL para metadatos de Airflow
- `postgres-etl` - Base de datos PostgreSQL para datos del ETL (separada)

### DAG del Pipeline ETL con Carga Paralela

El DAG `fuel_price_etl` ejecuta el pipeline completo diariamente a las 2 AM con **carga paralela** a PostgreSQL y Redshift:

#### Estructura del Flujo

```
extract → transform → [load_postgres, load_redshift]
                           ↓              ↓
                      PostgreSQL      Redshift
                      (staging)     (producción)
```

#### Tareas del DAG

1. **extract** - Descarga datos de APIs externas (Brent, USD/ARS, combustibles)
2. **transform** - Limpia, valida y transforma los datos, genera archivos Parquet
3. **load_postgres** - Carga a PostgreSQL local (staging) ⚡
4. **load_redshift** - Carga a AWS Redshift (producción) ⚡

> ⚡ Las tareas de carga se ejecutan **en paralelo**, optimizando el tiempo total del pipeline

#### Ventajas de la Carga Paralela

- **Optimización de tiempo:** Las cargas a PostgreSQL y Redshift ocurren simultáneamente
- **Independencia:** Si una carga falla, la otra continúa normalmente
- **Flexibilidad:** Puedes desactivar una carga sin afectar la otra
- **Escalabilidad:** Fácil agregar más destinos en paralelo

#### Destinos de Datos

**PostgreSQL (Staging Local)**
- Propósito: Base de datos local para desarrollo y staging
- Tablas staging: `brent_price`, `fuel_prices`, `usd_ars_rates`
- Tablas analytics: `brent_price_monthly`, `fuel_prices_monthly`, `usd_ars_rates_monthly`

**Redshift (Producción)**
- Propósito: Data warehouse en AWS para análisis en producción
- Schema: `2025_sebastian_castro_schema`
- Misma estructura de tablas que PostgreSQL
- Requiere configuración de credenciales (ver sección de Configuración de Redshift)

#### Ejecutar el DAG

**Desde la UI de Airflow:**
1. Ir a http://localhost:8080
2. Buscar el DAG `fuel_price_etl`
3. Activar el DAG con el toggle
4. Clic en "Trigger DAG" para ejecutarlo manualmente
5. Monitorear ejecución en la vista Graph

**Desde la terminal:**
```bash
# Trigger manual del DAG completo
docker exec airflow_scheduler airflow dags trigger fuel_price_etl

# Ejecutar tareas individuales para testing
docker exec airflow_scheduler airflow tasks test fuel_price_etl extract 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl transform 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl load_postgres 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl load_redshift 2025-11-20
```

### Comandos Útiles de Airflow

```bash
# Listar todos los DAGs
docker exec airflow_webserver airflow dags list

# Ver estado de un DAG específico
docker exec airflow_webserver airflow dags state fuel_price_etl

# Ver lista de ejecuciones (DAG runs)
docker exec airflow_webserver airflow dags list-runs -d fuel_price_etl

# Ejecutar una tarea específica en modo test (no registra en Airflow)
docker exec airflow_scheduler airflow tasks test fuel_price_etl extract 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl transform 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl load_postgres 2025-11-20

# Ver logs de una tarea
docker exec airflow_scheduler airflow tasks logs fuel_price_etl extract <RUN_ID>

# Pausar/Activar un DAG
docker exec airflow_webserver airflow dags pause fuel_price_etl
docker exec airflow_webserver airflow dags unpause fuel_price_etl

# Trigger manual de un DAG
docker exec airflow_scheduler airflow dags trigger fuel_price_etl

# Ver configuración de variables
docker exec airflow_scheduler airflow variables list
docker exec airflow_scheduler airflow variables get fuel_etl_update_all

# Setear variables
docker exec airflow_scheduler airflow variables set fuel_etl_update_all true --json
docker exec airflow_scheduler airflow variables set fuel_etl_brent_start_date "2023-01-01"

# Ver grafo de dependencias del DAG
docker exec airflow_scheduler airflow dags show fuel_price_etl
```

### Monitoreo y Logs

**Ver logs de Airflow:**
```bash
# Logs del scheduler
docker logs -f airflow_scheduler

# Logs del webserver
docker logs -f airflow_webserver

# Logs en archivos (dentro del contenedor)
docker exec airflow_scheduler ls -la /opt/airflow/logs/
```

**Verificar salud de Airflow:**
```bash
# Ver procesos corriendo
docker-compose ps

# Ver estado de salud de contenedores
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Verificar que Airflow puede conectarse a la BD
docker exec airflow_scheduler airflow db check
```

### Configuración de Airflow Variables

El DAG `fuel_price_etl` utiliza las siguientes variables opcionales que se pueden configurar desde la UI de Airflow para personalizar el comportamiento del pipeline:

| Variable | Tipo | Default | Descripción |
|----------|------|---------|-------------|
| `fuel_etl_update_all` | Boolean (JSON) | `true` | Si es `true`, actualiza todos los datos de combustibles desde la API. Si es `false`, solo actualiza datos incrementales |
| `fuel_etl_brent_start_date` | String | `"2022-01-01"` | Fecha de inicio (YYYY-MM-DD) para la extracción de precios de Brent desde Yahoo Finance |

**Para configurar en Airflow UI:**
1. Acceder a http://localhost:8080
2. Ir a **Admin** → **Variables**
3. Hacer clic en **+** (Add a new record)
4. Ingresar los datos:
   - **Key:** `fuel_etl_update_all`
   - **Val:** `true` (marcar checkbox "Is JSON" si está disponible)
5. Repetir para otras variables

**Para configurar desde CLI:**
```bash
# Configurar update_all (Boolean JSON)
docker exec airflow_scheduler airflow variables set fuel_etl_update_all true --json

# Configurar fecha de inicio de Brent (String)
docker exec airflow_scheduler airflow variables set fuel_etl_brent_start_date "2023-01-01"

# Ver todas las variables configuradas
docker exec airflow_scheduler airflow variables list

# Ver una variable específica
docker exec airflow_scheduler airflow variables get fuel_etl_update_all
```

**Notas:**
- Si no se configuran estas variables, el DAG usa los valores por defecto
- Los cambios en las variables se aplican en la **próxima ejecución** del DAG
- Para aplicar cambios inmediatamente, dispara manualmente el DAG después de modificar las variables

### Configuración de Redshift (Opcional)

Para habilitar la carga a AWS Redshift, configura las credenciales en el archivo `.env`:

```bash
# Agregar al archivo .env
REDSHIFT_CONNECTION_STRING=postgresql://user:password@cluster.region.redshift.amazonaws.com:5439/database_name
```

**Formato de la connection string:**
```
postgresql://[usuario]:[contraseña]@[cluster].[region].redshift.amazonaws.com:[puerto]/[database]
```

**Ejemplo:**
```bash
REDSHIFT_CONNECTION_STRING=postgresql://admin:MyPass123@my-cluster.us-east-1.redshift.amazonaws.com:5439/pda
```

**Notas importantes sobre Redshift:**
- ⚠️ Si no configuras `REDSHIFT_CONNECTION_STRING`, la tarea `load_redshift` fallará
- ✅ La tarea `load_postgres` funciona independientemente de Redshift
- 🔒 El schema usado es: `2025_sebastian_castro_schema`
- 📊 Se crean las mismas tablas que en PostgreSQL (staging + analytics)

**Desactivar carga a Redshift temporalmente:**

Si no tienes credenciales de Redshift, puedes:
1. Desde la UI de Airflow, marcar la tarea `load_redshift` como "skipped"
2. O comentar/eliminar temporalmente la tarea del DAG

### Validación del DAG

**Validar estructura del DAG desde CLI:**
```bash
# Ver lista de DAGs registrados
docker exec airflow_webserver airflow dags list | grep fuel

# Verificar que el DAG no tenga errores de importación
docker exec airflow_webserver airflow dags list-import-errors

# Ver grafo de dependencias del DAG
docker exec airflow_scheduler airflow dags show fuel_price_etl
```

**Validar con tests de Python:**
```bash
# Ejecutar tests de extracción y transformación
poetry run pytest tests/

# Ejecutar con cobertura
poetry run pytest --cov=src/fuel_price

# Verificar tipos con mypy
poetry run mypy src/fuel_price

# Verificar estilo con flake8
poetry run flake8 src/ tests/ dags/

# Formatear código con black
poetry run black src/ tests/ dags/
```

**Probar tareas del DAG individualmente:**
```bash
# Ejecutar cada tarea en modo test (no registra en Airflow)
docker exec airflow_scheduler airflow tasks test fuel_price_etl extract 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl transform 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl load_postgres 2025-11-20
docker exec airflow_scheduler airflow tasks test fuel_price_etl load_redshift 2025-11-20
```

**Estructura esperada del DAG:**
```
extract → transform → [load_postgres, load_redshift]
```

Las cargas a PostgreSQL y Redshift deben ejecutarse en **paralelo** (sin dependencias entre sí).

##  Autor

Sebastian J. Castro - [GitHub](https://github.com/sebacastrocba)

## 📄 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.
