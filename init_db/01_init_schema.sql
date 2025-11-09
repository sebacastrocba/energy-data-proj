-- Crear esquemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Comentarios descriptivos
COMMENT ON SCHEMA staging IS 'Área de staging - datos crudos o mínimamente procesados';
COMMENT ON SCHEMA analytics IS 'Área de analytics - datos transformados y agregados';

-- =============================================================================
-- STAGING SCHEMA - Datos crudos/mínimamente procesados
-- =============================================================================

-- Tabla staging para precios de Brent
CREATE TABLE IF NOT EXISTS staging.brent_prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    brent_price_usd DECIMAL(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

COMMENT ON TABLE staging.brent_prices IS 'Precios históricos del petróleo Brent en USD';

-- Tabla staging para precios de combustibles
CREATE TABLE IF NOT EXISTS staging.fuel_prices (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    provincia VARCHAR(100),
    bandera VARCHAR(200),
    producto VARCHAR(100),
    precio_surtidor DECIMAL(10, 2),
    volumen DECIMAL(15, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging.fuel_prices IS 'Precios de combustibles en Argentina por estación de servicio';

-- Tabla staging para cotización USD/ARS
CREATE TABLE IF NOT EXISTS staging.usd_ars_rates (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    usd_ars_oficial DECIMAL(10, 2),
    usd_ars_blue DECIMAL(10, 2),
    brecha_cambiaria_pct DECIMAL(10, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(fecha)
);

COMMENT ON TABLE staging.usd_ars_rates IS 'Cotización histórica USD/ARS oficial y blue';

-- =============================================================================
-- ANALYTICS SCHEMA - Datos transformados y agregados
-- =============================================================================

-- Tabla analytics para Brent (agregado mensual)
CREATE TABLE IF NOT EXISTS analytics.brent_prices_monthly (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    avg_brent_price_usd DECIMAL(10, 2) NOT NULL,
    min_brent_price_usd DECIMAL(10, 2),
    max_brent_price_usd DECIMAL(10, 2),
    record_count INTEGER,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month)
);

COMMENT ON TABLE analytics.brent_prices_monthly IS 'Precios de Brent agregados mensualmente';

-- Tabla analytics para combustibles (agregado mensual por provincia y producto)
CREATE TABLE IF NOT EXISTS analytics.fuel_prices_monthly (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    provincia VARCHAR(100) NOT NULL,
    bandera VARCHAR(200) NOT NULL,
    producto VARCHAR(100) NOT NULL,
    precio_surtidor_mediana DECIMAL(10, 2),
    volumen_total DECIMAL(15, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, provincia, bandera, producto)
);

COMMENT ON TABLE analytics.fuel_prices_monthly IS 'Precios de combustibles agregados mensualmente';

-- Tabla analytics para USD/ARS (agregado mensual)
CREATE TABLE IF NOT EXISTS analytics.usd_ars_rates_monthly (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    avg_usd_ars_oficial DECIMAL(10, 2),
    avg_usd_ars_blue DECIMAL(10, 2),
    avg_brecha_cambiaria_pct DECIMAL(10, 2),
    record_count INTEGER,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month)
);

COMMENT ON TABLE analytics.usd_ars_rates_monthly IS 'Cotización USD/ARS agregada mensualmente';

-- =============================================================================
-- ÍNDICES PARA MEJORAR PERFORMANCE
-- =============================================================================

-- Índices en staging
CREATE INDEX IF NOT EXISTS idx_brent_date ON staging.brent_prices(date);
CREATE INDEX IF NOT EXISTS idx_fuel_periodo ON staging.fuel_prices(periodo);
CREATE INDEX IF NOT EXISTS idx_fuel_producto ON staging.fuel_prices(producto);
CREATE INDEX IF NOT EXISTS idx_fuel_provincia ON staging.fuel_prices(provincia);
CREATE INDEX IF NOT EXISTS idx_usd_fecha ON staging.usd_ars_rates(fecha);

-- Índices en analytics
CREATE INDEX IF NOT EXISTS idx_brent_monthly_year_month ON analytics.brent_prices_monthly(year, month);
CREATE INDEX IF NOT EXISTS idx_fuel_monthly_year_month ON analytics.fuel_prices_monthly(year, month);
CREATE INDEX IF NOT EXISTS idx_fuel_monthly_producto ON analytics.fuel_prices_monthly(producto);
CREATE INDEX IF NOT EXISTS idx_usd_monthly_year_month ON analytics.usd_ars_rates_monthly(year, month);

-- =============================================================================
-- MENSAJE DE INICIALIZACIÓN EXITOSA
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Base de datos inicializada exitosamente';
    RAISE NOTICE 'Esquemas creados: staging, analytics';
    RAISE NOTICE 'Tablas staging: brent_prices, fuel_prices, usd_ars_rates';
    RAISE NOTICE 'Tablas analytics: brent_prices_monthly, fuel_prices_monthly, usd_ars_rates_monthly';
    RAISE NOTICE '=================================================================';
END $$;