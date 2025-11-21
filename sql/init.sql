-- Script de inicializacion de base de datos
-- Este archivo se ejecuta automaticamente cuando se crea el contenedor por primera vez

-- ============================================================================
-- CREAR ESQUEMAS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================================
-- TABLAS STAGING (datos crudos/limpios)
-- ============================================================================

-- Tabla: Precios de Brent
CREATE TABLE IF NOT EXISTS staging.brent_price (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    brent_price NUMERIC(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT brent_price_positive CHECK (brent_price > 0)
);

CREATE INDEX idx_brent_date ON staging.brent_price(date);

-- Tabla: Precios de combustibles (Secretaria de Energia)
CREATE TABLE IF NOT EXISTS staging.fuel_prices (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    provincia VARCHAR(100) NOT NULL,
    bandera VARCHAR(100),
    producto VARCHAR(100) NOT NULL,
    precio_surtidor NUMERIC(10, 2) NOT NULL,
    volumen NUMERIC(15, 2),
    market_share_pct NUMERIC(10, 4),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fuel_price_positive CHECK (precio_surtidor > 0)
);

CREATE INDEX idx_fuel_periodo ON staging.fuel_prices(periodo);
CREATE INDEX idx_fuel_producto ON staging.fuel_prices(producto);
CREATE INDEX idx_fuel_provincia ON staging.fuel_prices(provincia);

-- Tabla: Cotizaciones USD/ARS (blue y oficial)
CREATE TABLE IF NOT EXISTS staging.usd_ars_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    source VARCHAR(20) NOT NULL,  
    value_buy DECIMAL(10, 2),
    value_sell DECIMAL(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT usd_rates_positive CHECK (value_sell > 0),
    UNIQUE(date, source)  
);

CREATE INDEX idx_usd_date ON staging.usd_ars_rates(date);
CREATE INDEX idx_usd_source ON staging.usd_ars_rates(source);

-- ============================================================================
-- TABLAS ANALYTICS (datos agregados/transformados)
-- ============================================================================

-- Tabla: Precios de Brent agregados mensualmente
CREATE TABLE IF NOT EXISTS analytics.brent_prices_monthly (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    avg_brent_price NUMERIC(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT brent_monthly_positive CHECK (avg_brent_price > 0)
);

CREATE INDEX idx_brent_monthly_date ON analytics.brent_prices_monthly(date);

-- Tabla: Precios de combustibles agregados mensualmente
CREATE TABLE IF NOT EXISTS analytics.fuel_prices_monthly (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    producto VARCHAR(100) NOT NULL,
    precio_surtidor_mediana NUMERIC(10, 2) NOT NULL,
    volumen_total NUMERIC(15, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fuel_monthly_unique UNIQUE (periodo, producto),
    CONSTRAINT fuel_monthly_positive CHECK (precio_surtidor_mediana > 0)
);

CREATE INDEX idx_fuel_monthly_periodo ON analytics.fuel_prices_monthly(periodo);
CREATE INDEX idx_fuel_monthly_producto ON analytics.fuel_prices_monthly(producto);

-- Tabla: Cotizaciones USD/ARS agregadas mensualmente
CREATE TABLE IF NOT EXISTS analytics.usd_ars_rates_monthly (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    usd_ars_oficial NUMERIC(10, 2) NOT NULL,
    usd_ars_blue NUMERIC(10, 2) NOT NULL,
    brecha_cambiaria_pct NUMERIC(10, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT usd_monthly_positive CHECK (usd_ars_oficial > 0 AND usd_ars_blue > 0)
);

CREATE INDEX idx_usd_monthly_date ON analytics.usd_ars_rates_monthly(date);

-- ============================================================================
-- INFORMACION DE INICIALIZACION
-- ============================================================================

-- Insertar registro de cuando se creo la base
CREATE TABLE IF NOT EXISTS public.db_info (
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version VARCHAR(50) DEFAULT '1.0'
);

INSERT INTO public.db_info (created_at, version) 
VALUES (CURRENT_TIMESTAMP, '1.0');

-- Mensaje de confirmacion
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Base de datos inicializada correctamente';
    RAISE NOTICE 'Esquemas creados: staging, analytics';
    RAISE NOTICE 'Tablas staging: 3';
    RAISE NOTICE 'Tablas analytics: 3';
    RAISE NOTICE '========================================';
END $$;