# Tests para funciones de extracción

import pytest
import pandas as pd
from pda_project.extract import extract_brent_prices

def test_extract_brent_prices():
    """
    Test para la función extract_brent_prices.
    """

    result = extract_brent_prices()

    # Verificar que el resultado es un DataFrame
    assert isinstance(result, pd.DataFrame), "El resultado debe ser un DataFrame"

    # Verificar que no está vacio
    assert len(result) > 0, "El DataFrame no debe estar vacío"

    # Verificar que tenga columnas correctas
    assert 'date' in result.columns, "El DataFrame debe contener la columna 'date'"
    assert 'brent_price' in result.columns, "El DataFrame debe contener la columna 'brent_price'"


def test_extract_brent_prices_column_types():

    result = extract_brent_prices()

    # Verificar que los precios sean numéricos
    assert pd.api.types.is_numeric_dtype(result['brent_price']), "La columna 'brent_price' debe ser de tipo numérico"

    # Verificar que las fechas sean de tipo datetime
    assert pd.api.types.is_datetime64_any_dtype(result['date']), "La columna 'date' debe ser de tipo datetime"
    