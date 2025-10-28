# Tests para funciones de extracción

import pytest
import pandas as pd
import requests
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import zipfile
import io

class TestExtractBrentPrices:
    """Tests para la función extract_brent_prices"""
    
    @patch('yfinance.download')
    def test_extract_brent_returns_dataframe(self, mock_yf_download):
        """
        Test: Verifica que extract_brent_prices devuelva un DataFrame con la estructura correcta
        
        Este test usa @patch para simular la descarga de yfinance sin hacer llamadas reales
        """
        # Arrange: Crear datos de prueba
        mock_data = pd.DataFrame({
            'Close': [80.5, 81.2, 79.8],
        }, index=pd.date_range('2024-01-01', periods=3, freq='D'))
        mock_yf_download.return_value = mock_data
        
        # Act: Ejecutar la función
        from fuel_price.extract import extract_brent_prices
        result = extract_brent_prices(start_date="2024-01-01")
        
        # Assert: Verificar resultados
        assert isinstance(result, pd.DataFrame), "Debe devolver un DataFrame"
        assert len(result) == 3, "Debe tener 3 filas"
        assert list(result.columns) == ['date', 'brent_price'], "Columnas incorrectas"
        assert all(result['brent_price'] > 0), "Los precios deben ser positivos"
    
    @patch('yfinance.download')
    def test_extract_brent_column_types(self, mock_yf_download):
        """
        Test: Verifica que los tipos de datos sean correctos
        """
        # Arrange
        mock_data = pd.DataFrame({
            'Close': [80.5, 81.2],
        }, index=pd.date_range('2024-01-01', periods=2, freq='D'))
        mock_yf_download.return_value = mock_data
        
        # Act
        from fuel_price.extract import extract_brent_prices
        result = extract_brent_prices()
        
        # Assert
        assert pd.api.types.is_numeric_dtype(result['brent_price']), \
            "brent_price debe ser numérico"
        # La columna date puede ser datetime64 o object, ambos son válidos
        assert result['date'].dtype in ['datetime64[ns]', 'object'], \
            "date debe ser datetime o object"
    
    @patch('yfinance.download')
    def test_extract_brent_uses_correct_ticker(self, mock_yf_download):
        """
        Test: Verifica que se use el ticker correcto de Brent (BZ=F)
        """
        # Arrange
        mock_data = pd.DataFrame({
            'Close': [80.5],
        }, index=pd.date_range('2024-01-01', periods=1, freq='D'))
        mock_yf_download.return_value = mock_data
        
        # Act
        from fuel_price.extract import extract_brent_prices
        extract_brent_prices()
        
        # Assert: Verificar que se llamó con el ticker correcto
        mock_yf_download.assert_called_once()
        call_args = mock_yf_download.call_args
        assert call_args[0][0] == "BZ=F", "Debe usar el ticker BZ=F para Brent"