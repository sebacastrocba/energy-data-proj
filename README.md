# Análisis de Precios de Combustibles en Argentina

![CI](https://github.com/sebacastrocba/energy-data-proj/workflows/CI/badge.svg)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Descripción del Proyecto

Este proyecto implementa un pipeline de datos (ETL/ELT) que extrae, transforma y carga información sobre precios de combustibles en Argentina, combinándola con datos del precio internacional del petróleo Brent. El objetivo es analizar la correlación entre los precios locales y el mercado internacional de petróleo.

**Caso de uso:** Permite a analistas y tomadores de decisiones entender cómo fluctúan los precios de combustibles en Argentina en relación con el precio del petróleo Brent, identificando patrones temporales y geográficos.

## Características Principales

- ✅ **Extracción de datos** de múltiples fuentes (Secretaría de Energía, Yahoo Finance, Bluelytics)
- ✅ **Transformación y limpieza** de datos con pandas
- ✅ **Carga a PostgreSQL** con Docker Compose (schemas staging y analytics)
- ✅ **Análisis exploratorio** con Jupyter notebooks
- ✅ **Tests automatizados** con pytest (67 tests)
- ✅ **CI/CD con GitHub Actions** (tests + linting)
- ✅ **Gestión de dependencias** con Poetry

## 🚀 Inicio Rápido

### Requisitos Previos

- Docker y Docker Compose instalados
- Python 3.12+
- Poetry (recomendado) o pip

### Instalación Automática (Recomendado)

```bash
# 1. Clonar el repositorio
git clone https://github.com/sebacastrocba/energy-data-proj.git
cd fuel_price_project

# 2. Ejecutar script de configuración (configura PostgreSQL + dependencias)
./setup.sh
```

El script `setup.sh` automáticamente:
- ✅ Verifica Docker y Docker Compose
- ✅ Crea archivo `.env` desde `.env.example`
- ✅ Levanta PostgreSQL en Docker
- ✅ Inicializa el schema de la base de datos (staging + analytics)
- ✅ Instala dependencias Python con Poetry
- ✅ Verifica la conexión a PostgreSQL

### Instalación Manual

Si prefieres configurar manualmente:

```bash
# 1. Clonar repositorio
git clone https://github.com/sebacastrocba/energy-data-proj.git
cd fuel_price_project

# 2. Crear archivo de configuración
cp .env.example .env
# Edita .env si necesitas cambiar credenciales

# 3. Levantar PostgreSQL
docker-compose up -d

# 4. Inicializar schema de base de datos
# Nota: Usa las credenciales definidas en docker-compose.yml
PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db -f init_db/01_init_schema.sql

# 5. Instalar dependencias Python
poetry install
# O con pip: pip install -r requirements.txt
```

## 📊 Uso del Pipeline

### Opción 1: Pipeline Completo (Automático)

```bash
# Ejecutar todo el pipeline (Extract + Transform + Load)
poetry run python -m fuel_price.load
```

### Opción 2: Paso a Paso (Manual)

```bash
# Paso 1: Extraer datos de APIs
poetry run python -m fuel_price.extract

# Paso 2: Transformar datos (limpieza y agregaciones)
poetry run python -m fuel_price.transform

# Paso 3: Cargar a PostgreSQL
poetry run python -m fuel_price.load
```

### Opción 3: Usar como Librería

```python
from fuel_price.extract import extract_all_data
from fuel_price.transform import clean_brent_price, agg_brent_price_for_analytics
from fuel_price.load import load_brent_to_staging, load_brent_to_analytics

# Extraer
brent_raw, fuel_raw, usd_raw = extract_all_data(update_all=False)

# Transformar
brent_clean = clean_brent_price(brent_raw)
brent_analytics = agg_brent_price_for_analytics(brent_clean)

# Cargar a STAGING
load_brent_to_staging(brent_clean, truncate=True)

# Cargar a ANALYTICS
load_brent_to_analytics(brent_analytics, truncate=True)
```


## 🗂️ Estructura de Base de Datos

### Schemas Creados

- **`staging`**: Datos crudos o mínimamente procesados
- **`analytics`**: Datos transformados y agregados

### Tablas Staging

| Tabla | Descripción |
|-------|-------------|
| `staging.brent_prices` | Precios históricos del petróleo Brent |
| `staging.fuel_prices` | Precios de combustibles por estación |
| `staging.usd_ars_rates` | Cotización USD/ARS oficial y blue |

### Tablas Analytics

| Tabla | Descripción |
|-------|-------------|
| `analytics.brent_prices_monthly` | Brent agregado mensualmente |
| `analytics.fuel_prices_monthly` | Combustibles agregados por mes/provincia/producto |
| `analytics.usd_ars_rates_monthly` | USD/ARS agregado mensualmente |

## 🔍 Explorar la Base de Datos

### Conectar con psql

```bash
PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db
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
\d staging.brent_prices

-- Consultar datos
SELECT * FROM staging.brent_prices ORDER BY date DESC LIMIT 10;
SELECT * FROM analytics.fuel_prices_monthly WHERE year = 2024 LIMIT 10;
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

Ejecutar todos los tests:

```bash
poetry run pytest
```

Ejecutar tests con cobertura:

```bash
poetry run pytest --cov=src/fuel_price --cov-report=html
```

Ejecutar tests específicos:

```bash
# Solo tests de extracción
poetry run pytest tests/test_extract.py

# Solo tests de transformación
poetry run pytest tests/test_transform.py
```

## 📁 Estructura del Proyecto

```
fuel_price_project/
├── src/
│   └── fuel_price/
│       ├── extract.py          # Extracción de datos de APIs
│       ├── transform.py        # Transformación y limpieza
│       ├── load.py             # Carga a PostgreSQL
│       ├── config.py           # Configuración global
│       └── get_price_data_SE_linux.py  # Extractor de SE
├── tests/
│   ├── test_extract.py         # Tests de extracción
│   └── test_transform.py       # Tests de transformación
├── notebooks/
│   └── 00_analisis_exploratorio.ipynb  # Análisis en Jupyter
├── init_db/
│   └── 01_init_schema.sql      # Schema SQL para PostgreSQL
├── data/
│   ├── raw/                    # Datos originales (gitignored)
│   └── processed/              # Datos procesados (gitignored)
├── docker-compose.yml          # Configuración de PostgreSQL
├── setup.sh                    # Script de inicialización
├── load_example.py             # Ejemplo de carga a PostgreSQL
├── .env.example                # Plantilla de variables de entorno
└── pyproject.toml              # Configuración de Poetry
```

## 📚 Fuentes de Datos

- **Precios de Combustibles:** [Secretaría de Energía de Argentina](http://res1104.se.gob.ar/)
- **Precio del Brent:** Yahoo Finance (símbolo: BZ=F)
- **Tipo de Cambio USD/ARS:** [Bluelytics API](https://bluelytics.com.ar/#!/api)

## 🛠️ Solución de Problemas

### PostgreSQL no inicia

```bash
# Ver logs del contenedor
docker-compose logs postgres

# Reiniciar desde cero
docker-compose down -v
./setup.sh
```

### Error de conexión a PostgreSQL

```bash
# Verificar que el contenedor esté corriendo
docker-compose ps

# Probar conexión
poetry run python -c "from src.fuel_price.load import test_connection; test_connection()"
```

### El schema SQL no se ejecutó

```bash
# Ejecutar manualmente
PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db -f init_db/01_init_schema.sql
```

## 📝 Notas Importantes

- **Puerto:** PostgreSQL corre en `15432` (no en el estándar 5432) para evitar conflictos
- **Credenciales:** Las credenciales en este README son de ejemplo para desarrollo local. Para producción, usa variables de entorno seguras
- **Volúmenes:** Los datos persisten en un volumen Docker
- **Schemas:** Usa `staging` para datos crudos y `analytics` para agregados
- **Reinicializar:** Para borrar todo, usa `docker-compose down -v` y ejecuta `./setup.sh` nuevamente
- **Datos:** Los archivos CSV grandes están en `.gitignore` y no se suben al repositorio

## 👤 Autor

Sebastian J. Castro - [GitHub](https://github.com/sebacastrocba)

## 📄 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.
