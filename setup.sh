#!/bin/bash

# Script para inicializar el ambiente de desarrollo

set -e

echo "======================================================================"
echo "INICIALIZACION DEL PROYECTO - FUEL PRICES ETL"
echo "======================================================================"

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para imprimir mensajes
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. Verificar Docker
print_info "Verificando instalacion de Docker..."
if ! command -v docker &> /dev/null; then
    print_error "Docker no esta instalado. Instala Docker primero."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose no esta instalado. Instala docker-compose primero."
    exit 1
fi

print_info "Docker y docker-compose estan instalados"

# 2. Crear archivo .env si no existe
if [ ! -f .env ]; then
    print_info "Creando archivo .env desde .env.example..."
    cp .env.example .env
    print_warning "Revisa el archivo .env y ajusta las credenciales si es necesario"
else
    print_info "Archivo .env ya existe"
fi

# 3. Levantar PostgreSQL
print_info "Levantando PostgreSQL con Docker Compose..."
docker-compose up -d

# 4. Esperar a que PostgreSQL esté listo
print_info "Esperando a que PostgreSQL este listo..."
sleep 5

# Verificar que PostgreSQL este corriendo usando healthcheck
max_attempts=30
attempt=0
until docker-compose ps | grep "healthy" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        print_error "Timeout esperando a PostgreSQL"
        exit 1
    fi
    print_info "Esperando PostgreSQL... ($attempt/$max_attempts)"
    sleep 2
done

print_info "PostgreSQL esta listo"

# 5. Inicializar schema de base de datos
print_info "Inicializando schema de base de datos..."
if [ -f "init_db/01_init_schema.sql" ]; then
    PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db -f init_db/01_init_schema.sql
    if [ $? -eq 0 ]; then
        print_info "Schema inicializado correctamente"
    else
        print_error "Error al inicializar schema"
        exit 1
    fi
else
    print_warning "No se encontro init_db/01_init_schema.sql"
fi

# 6. Verificar instalación de Poetry
print_info "Verificando Poetry..."
if ! command -v poetry &> /dev/null; then
    print_warning "Poetry no esta instalado. Instalando dependencias con pip..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    else
        print_error "No se encontro requirements.txt"
        exit 1
    fi
else
    print_info "Instalando dependencias con Poetry..."
    poetry install
fi

# 7. Verificar conexión
print_info "Verificando conexion a PostgreSQL..."
export $(grep -v '^#' .env | xargs)
poetry run python -c "from src.fuel_price.load import test_connection; import sys; sys.exit(0 if test_connection() else 1)"
if [ $? -eq 0 ]; then
    print_info "Conexion exitosa a PostgreSQL"
else
    print_error "No se pudo conectar a PostgreSQL"
    exit 1
fi

# 7. Información final
echo ""
echo "======================================================================"
print_info "INICIALIZACION COMPLETADA EXITOSAMENTE"
echo "======================================================================"
echo ""
print_info "PostgreSQL corriendo en: localhost:15432"
print_info "Base de datos: fuel_prices_db"
print_info "Usuario: fuel_user"
print_info "Schemas: staging, analytics"
echo ""
print_info "Comandos utiles:"
echo "  - Ver logs: docker-compose logs -f"
echo "  - Detener: docker-compose stop"
echo "  - Reiniciar: docker-compose restart"
echo "  - Eliminar (BORRA DATOS): docker-compose down -v"
echo ""
print_info "Conectarse con psql:"
echo "  PGPASSWORD=fuel_pass psql -h localhost -p 15432 -U fuel_user -d fuel_prices_db"
echo ""
print_info "Para inspeccionar la base de datos:"
echo "  poetry run python inspect_db.py"
echo ""
print_info "Para probar la carga de datos:"
echo "  poetry run python -m fuel_price.load"
echo ""
echo "======================================================================"