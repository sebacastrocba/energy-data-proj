# Análisis de Precios de Combustibles en Argentina

![CI](https://github.com/sebacastrocba/energy-data-proj/workflows/CI/badge.svg)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Descripción del Proyecto

Este proyecto implementa un pipeline de datos (ETL) completo que extrae, transforma y carga información sobre precios de combustibles en Argentina, combinándola con datos del precio internacional del petróleo Brent y cotizaciones USD/ARS (oficial y blue). El objetivo es analizar la correlación entre los precios locales, el mercado internacional de petróleo y la evolución del tipo de cambio.

**Caso de uso:** Permite a analistas y tomadores de decisiones entender cómo fluctúan los precios de combustibles en Argentina en relación con el precio del petróleo Brent y el tipo de cambio, identificando patrones temporales y geográficos.

## Características Principales

- ✅ **Extracción de datos** de múltiples fuentes (Secretaría de Energía, Yahoo Finance, Bluelytics API)
- ✅ **Transformación y limpieza** de datos con pandas y numpy
- ✅ **Carga a PostgreSQL** con Docker Compose (schemas staging y analytics)
- ✅ **Orquestación con Apache Airflow** para automatización del pipeline
- ✅ **Análisis exploratorio** con Jupyter notebooks
- ✅ **Tests automatizados** con pytest y pytest-cov
- ✅ **Type checking** con mypy
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
│       └── get_price_data_SE.py # Extractor de datos de Secretaría de Energía
├── tests/
│   ├── __init__.py
│   ├── test_extract.py          # Tests de extracción
│   └── test_transform.py        # Tests de transformación
├── notebooks/
│   └── 00_analisis_exploratorio.ipynb  # Análisis exploratorio de datos
├── sql/
│   └── init.sql                 # Schema SQL (staging + analytics)
├── data/
│   ├── raw/                     # Datos originales (gitignored)
│   │   ├── brent_prices.csv
│   │   ├── usd_ars_bluelytics.csv
│   │   └── precios_eess_completo.csv
│   └── processed/               # Datos procesados (gitignored)
├── logs/                        # Logs del sistema (gitignored)
├── docs/                        # Documentación adicional
├── dags/                        # Para futura integración con Airflow
├── docker-compose.yml           # Configuración de PostgreSQL
├── pyproject.toml               # Configuración de Poetry y dependencias
├── mypy.ini                     # Configuración de mypy
├── .env.example                 # Plantilla de variables de entorno
└── README.md                    # Este archivo
```

## 📚 Fuentes de Datos

- **Precios de Combustibles:** [Secretaría de Energía de Argentina](http://res1104.se.gob.ar/) - Datos históricos desde 2022
- **Precio del Brent:** [Yahoo Finance](https://finance.yahoo.com/) (símbolo: BZ=F) - Precios diarios
- **Tipo de Cambio USD/ARS:** [Bluelytics API](https://bluelytics.com.ar/#!/api) - Cotizaciones oficial y blue

## 🔧 Tecnologías Utilizadas

- **Lenguaje:** Python 3.12
- **Gestión de dependencias:** Poetry
- **Base de datos:** PostgreSQL 15 (Alpine)
- **Contenedores:** Docker y Docker Compose
- **Análisis de datos:** pandas, numpy
- **Testing:** pytest, pytest-cov, pytest-mock
- **Type checking:** mypy
- **Formato de datos:** Parquet (PyArrow)

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

## 🤖 Orquestación con Apache Airflow

### Configuración Inicial de Airflow

Airflow ya está instalado y configurado. Para iniciar Airflow:

```bash
# Opción 1: Modo standalone (webserver + scheduler en un solo proceso)
./start_airflow.sh

# Opción 2: Componentes separados (en terminales diferentes)
./start_webserver.sh   # Terminal 1: UI Web
./start_scheduler.sh   # Terminal 2: Scheduler
```

**Acceso a la UI:**
- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin123`

### Configuración de Airflow

Airflow está configurado con:
- **Base de datos:** PostgreSQL (misma instancia que los datos)
- **Ejecutor:** LocalExecutor (permite paralelismo)
- **DAGs folder:** `/dags`
- **Ejemplos:** Desactivados (`load_examples = False`)

### DAG del Pipeline ETL

El DAG `fuel_price_etl` ejecuta el pipeline completo diariamente a las 2 AM:

1. **Extract:** Descarga datos de APIs externas
2. **Transform:** Limpia y transforma los datos
3. **Load:** Carga a PostgreSQL

Para ejecutar manualmente desde la UI:
1. Ir a http://localhost:8080
2. Activar el DAG con el toggle
3. Clic en "Trigger DAG" para ejecutarlo inmediatamente

Para ejecutar desde la terminal:
```bash
export AIRFLOW_HOME=$PWD
poetry run airflow dags trigger fuel_price_etl
```

### Comandos Útiles de Airflow

```bash
# Listar DAGs
export AIRFLOW_HOME=$PWD && poetry run airflow dags list

# Ver estado de un DAG
poetry run airflow dags state fuel_price_etl

# Ejecutar una tarea específica
poetry run airflow tasks test fuel_price_etl extract 2024-01-01

# Ver logs de una tarea
poetry run airflow tasks logs fuel_price_etl extract 2024-01-01
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

## 👤 Autor

Sebastian J. Castro - [GitHub](https://github.com/sebacastrocba)

## 📄 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.
