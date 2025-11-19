import pytest
import pandas as pd
import requests
from unittest.mock import patch
from fuel_price.extract import (
    extract_brent_prices,
    extract_dolar_bluelytics,
    extract_fuel_prices,
)


# 1. Test para verificar que extract_brent_prices maneja datos vacios correctamente
@patch("fuel_price.extract.yf.download")
def test_extract_brent_prices_empty_data(mock_download, tmp_path):
    """
    Test que verifica que la función extract_brent_prices maneja correctamente datos vacíos.
    """
    # Configuar el mock
    mock_download.return_value = pd.DataFrame()

    # Verificar que se lanza ValueError al recibir datos vacíos
    with pytest.raises(ValueError, match="No se obtuvieron datos de Brent"):
        extract_brent_prices(
            start_date="2024-01-01", end_date="2024-01-03", output_path=tmp_path
        )


# 2. Test para verificar que extract_dolar_bluelytics maneja errores de conexión
@patch("fuel_price.extract.requests.get")
def test_extract_dolar_bluelytics_connection_error(mock_get, tmp_path):
    """
    Test que verifica que la función extract_dolar_bluelytics maneja errores de conexión.
    """
    # Configuar el mock para simular un error de conexión
    mock_get.side_effect = requests.exceptions.RequestException("Error de conexión")

    # Verificar que se lanza al ocurrir un error de conexión
    with pytest.raises(ValueError, match="Error al obtener datos de Bluelytics"):
        extract_dolar_bluelytics(
            start_date="2024-01-01", end_date="2024-01-03", output_path=tmp_path
        )


# 3. Test para verificar que extract_fuel_prices maneja archivo inexistente
def test_extract_fuel_prices_file_not_found(tmp_path):
    """
    Test que verifica que la función extract_fuel_prices maneja el caso de archivo inexistente.
    """

    # Verificar que se lanza FileNotFoundError al intentar leer un archivo inexistente
    with pytest.raises(FileNotFoundError):
        extract_fuel_prices(data_path=tmp_path, update_data=False)
