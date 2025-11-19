# Tests unitarios para funciones de transformación

import pytest
import pandas as pd
import numpy as np

from fuel_price.transform import (
    agg_brent_price,
    fuel_price_aggs,
    dollar_price_aggs,
)


# 1. Test para verificar que agg_brent_price calcula el promedio mensual correctamente
def test_agg_brent_price_calculates_monthly_average():
    # Datos de ejemplo diarios con valores FIJOS (no aleatorios)
    data = {
        "date": pd.date_range(start="2022-01-01", end="2022-01-05", freq="D"),
        "brent_price": [80.0, 85.0, 90.0, 75.0, 70.0],
    }
    df = pd.DataFrame(data)

    # Aplicar la función
    result = agg_brent_price(df)

    # Verificar que el resultado tiene una fila (un mes)
    assert len(result) == 1
    # Verificar que el promedio es correcto
    expected_avg = 80.0  # (80+85+90+75+70)/5
    assert np.isclose(result.iloc[0]["brent_price"], expected_avg)


# 2. Test para verificar que fuel_price_aggs agrupa y calcula medianas y sumas correctamente
def test_fuel_price_aggs_groups_and_calculates_correctly():
    # Datos de ejemplo con DOS productos diferentes para generar 2 grupos
    data = {
        "periodo": pd.to_datetime(["2022-01-01", "2022-01-01", "2022-01-01"]),
        "provincia": ["Buenos Aires", "Buenos Aires", "Córdoba"],
        "bandera": ["YPF", "YPF", "Shell"],
        "producto": [
            "NAFTA GRADO 2",
            "NAFTA GRADO 2",
            "GASOIL GRADO 3",
        ],  # ← Producto diferente
        "precio_surtidor": [100.0, 120.0, 110.0],
        "volumen": [1000, 2000, 1500],
    }
    df = pd.DataFrame(data)

    # Aplicar la función
    result = fuel_price_aggs(df)

    # Verificar que hay DOS grupos (agrupa solo por periodo + producto)
    assert len(result) == 2, f"Esperaba 2 grupos, obtuvo {len(result)}"

    # Verificar cálculos para NAFTA GRADO 2 (las primeras 2 filas)
    nafta_group = result[result["producto"] == "NAFTA GRADO 2"]

    assert len(nafta_group) == 1, "Debe haber 1 grupo para NAFTA GRADO 2"

    # Mediana de [100.0, 120.0] = 110.0
    expected_median = 110.0
    # Suma de volúmenes: 1000 + 2000 = 3000
    expected_volume = 3000

    assert np.isclose(nafta_group.iloc[0]["precio_surtidor_mediana"], expected_median)
    assert nafta_group.iloc[0]["volumen_total"] == expected_volume

    # Verificar cálculos para GASOIL GRADO 3
    gasoil_group = result[result["producto"] == "GASOIL GRADO 3"]

    assert len(gasoil_group) == 1, "Debe haber 1 grupo para GASOIL GRADO 3"
    assert gasoil_group.iloc[0]["precio_surtidor_mediana"] == 110.0
    assert gasoil_group.iloc[0]["volumen_total"] == 1500


# 3. Test para verificar que dollar_price_aggs pivotea y calcula brecha correctamente
def test_dollar_price_aggs_pivots_and_calculates_brecha():
    # Datos de ejemplo con valores controlados
    data = {
        "date": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        "source": ["Oficial", "Blue", "Oficial", "Blue"],
        "value_sell": [100.0, 180.0, 102.0, 184.0],
        "value_buy": [98.0, 178.0, 100.0, 182.0],
    }
    df = pd.DataFrame(data)

    # Aplicar la función
    result = dollar_price_aggs(df)

    # Verificar que las columnas pivotadas existen
    assert "usd_ars_oficial" in result.columns
    assert "usd_ars_blue" in result.columns
    assert "brecha_cambiaria_pct" in result.columns

    # Calcular valores esperados DESPUÉS del resample mensual
    expected_oficial = (100.0 + 102.0) / 2  # = 101.0
    expected_blue = (180.0 + 184.0) / 2  # = 182.0
    expected_brecha = ((expected_blue - expected_oficial) / expected_oficial) * 100
    # = ((182-101)/101)*100 ≈ 80.198%

    assert np.isclose(result.iloc[0]["usd_ars_oficial"], expected_oficial)
    assert np.isclose(result.iloc[0]["usd_ars_blue"], expected_blue)
    assert np.isclose(
        result.iloc[0]["brecha_cambiaria_pct"], expected_brecha, rtol=0.01
    )
