import pytest
import pandas as pd
from fuel_price.transform import clean_brent_price

def test_clean_brent_removes_nulls():
    """
    Test that clean_brent_price removes rows with null values.
    """

    # Crear un DataFrame de prueba con valores nulos
    data = {
        "date": ["2022-01-01", "2022-01-02", None, "2022-01-04"],
        "brent_price": [80.5, None, 82.0, 83.5]
    }
    
    df = pd.DataFrame(data)

    # Aplicar la función de limpieza
    cleaned_df = clean_brent_price(df)

    # Verificar que las filas con valores nulos han sido eliminadas
    assert cleaned_df.isnull().sum().sum() == 0