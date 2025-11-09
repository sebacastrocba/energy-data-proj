# Tests para funciones de extracción

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import subprocess
import requests

from fuel_price.extract import (
    get_today_date,
    find_csv_files,
    get_default_data_path,
    get_project_root,
    run_download_script,
    extract_brent_prices,
    extract_fuel_prices,
    extract_dolar_bluelytics,
    extract_all_data
)


# ============================================================================
# Tests para funciones auxiliares
# ============================================================================

class TestAuxiliaryFunctions:
    """Tests para funciones auxiliares."""
    
    def test_get_project_root(self):
        """Test que get_project_root retorna un Path válido."""
        root = get_project_root()
        assert isinstance(root, Path)
        assert root.exists()
        # Verificar que contiene archivos típicos de proyecto
        assert (root / "src").exists() or (root / "tests").exists()
    
    def test_get_default_data_path(self):
        """Test que get_default_data_path retorna data/raw."""
        data_path = get_default_data_path()
        assert isinstance(data_path, Path)
        assert data_path.name == "raw"
        assert data_path.parent.name == "data"
    
    def test_get_today_date_format(self):
        """Test que get_today_date retorna formato YYYY-MM-DD."""
        today = get_today_date()
        assert isinstance(today, str)
        # Verificar formato con regex
        assert len(today) == 10
        assert today[4] == "-" and today[7] == "-"
        # Verificar que es parseable
        datetime.strptime(today, "%Y-%m-%d")
    
    def test_find_csv_files_with_pattern(self, tmp_path):
        """Test que find_csv_files encuentra archivos con patrón."""
        # Crear archivos de prueba
        (tmp_path / "precios_2024.csv").touch()
        (tmp_path / "precios_2025.csv").touch()
        (tmp_path / "otros_datos.csv").touch()
        
        files = find_csv_files(tmp_path, pattern="*precios*.csv")
        assert len(files) == 2
        assert all(f.suffix == ".csv" for f in files)
        assert all("precios" in f.name for f in files)
    
    def test_find_csv_files_empty_directory(self, tmp_path):
        """Test que find_csv_files retorna lista vacía si no hay archivos."""
        files = find_csv_files(tmp_path)
        assert files == []
    
    @patch("subprocess.run")
    def test_run_download_script(self, mock_run):
        """Test que run_download_script ejecuta el script correctamente."""
        script_path = Path("/fake/path/script.py")
        
        run_download_script(script_path)
        
        mock_run.assert_called_once_with(
            ["python", str(script_path)],
            check=True
        )


# ============================================================================
# Tests para extract_brent_prices
# ============================================================================

class TestExtractBrentPrices:
    """Tests para la función extract_brent_prices."""
    
    @patch("fuel_price.extract.yf.download")
    def test_extract_brent_prices_success(self, mock_download, tmp_path):
        """Test extracción exitosa de precios de Brent."""
        # Mock de datos de yfinance
        mock_data = pd.DataFrame({
            "Close": [80.5, 81.2, 82.0]
        }, index=pd.date_range("2024-01-01", periods=3))
        mock_download.return_value = mock_data
        
        # Ejecutar
        result = extract_brent_prices(
            start_date="2024-01-01",
            end_date="2024-01-03",
            output_path=tmp_path
        )
        
        # Verificaciones
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["date", "brent_price_usd"]
        assert result["brent_price_usd"].tolist() == [80.5, 81.2, 82.0]
        
        # Verificar que se guardó el archivo
        saved_file = tmp_path / "brent_prices.csv"
        assert saved_file.exists()
        
        # Verificar llamada a yfinance
        mock_download.assert_called_once_with(
            "BZ=F",
            start="2024-01-01",
            end="2024-01-03",
            progress=False
        )
    
    @patch("fuel_price.extract.yf.download")
    def test_extract_brent_prices_with_defaults(self, mock_download, tmp_path):
        """Test que usa fecha de hoy por defecto."""
        mock_data = pd.DataFrame({
            "Close": [80.5]
        }, index=pd.date_range("2024-01-01", periods=1))
        mock_download.return_value = mock_data
        
        with patch("fuel_price.extract.get_today_date", return_value="2024-12-31"):
            with patch("fuel_price.extract.get_default_data_path", return_value=tmp_path):
                result = extract_brent_prices(start_date="2024-01-01")
        
        # Verificar que usó fecha de hoy
        assert mock_download.call_args[1]["end"] == "2024-12-31"
    
    @patch("fuel_price.extract.yf.download")
    def test_extract_brent_prices_empty_data(self, mock_download, tmp_path):
        """Test que levanta error cuando no hay datos."""
        mock_download.return_value = pd.DataFrame()
        
        with pytest.raises(ValueError, match="No se obtuvieron datos de Brent"):
            extract_brent_prices(
                start_date="2024-01-01",
                end_date="2024-01-03",
                output_path=tmp_path
            )
    
    @patch("fuel_price.extract.yf.download")
    def test_extract_brent_prices_creates_directory(self, mock_download, tmp_path):
        """Test que crea directorios si no existen."""
        mock_data = pd.DataFrame({
            "Close": [80.5]
        }, index=pd.date_range("2024-01-01", periods=1))
        mock_download.return_value = mock_data
        
        nested_path = tmp_path / "level1" / "level2" / "level3"
        
        extract_brent_prices(
            start_date="2024-01-01",
            end_date="2024-01-01",
            output_path=nested_path
        )
        
        assert nested_path.exists()
        assert (nested_path / "brent_prices.csv").exists()


# ============================================================================
# Tests para extract_fuel_prices
# ============================================================================

class TestExtractFuelPrices:
    """Tests para la función extract_fuel_prices."""
    
    @patch("fuel_price.extract.run_download_script")
    def test_extract_fuel_prices_with_update(self, mock_run_script, tmp_path):
        """Test extracción de combustibles con actualización."""
        # Crear archivos CSV de prueba
        df1 = pd.DataFrame({
            "fecha": ["2024-01-01", "2024-01-02"],
            "precio": [100, 101]
        })
        df2 = pd.DataFrame({
            "fecha": ["2024-01-03"],
            "precio": [102]
        })
        
        df1.to_csv(tmp_path / "precios_2024.csv", index=False)
        df2.to_csv(tmp_path / "precios_2025.csv", index=False)
        
        # Ejecutar
        result = extract_fuel_prices(data_path=tmp_path, update_data=True)
        
        # Verificaciones
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_run_script.assert_called_once()
    
    @patch("fuel_price.extract.run_download_script")
    def test_extract_fuel_prices_without_update(self, mock_run_script, tmp_path):
        """Test extracción sin actualización (solo lectura)."""
        # Crear archivo CSV de prueba
        df = pd.DataFrame({
            "fecha": ["2024-01-01"],
            "precio": [100]
        })
        df.to_csv(tmp_path / "precios_2024.csv", index=False)
        
        # Ejecutar sin actualizar
        result = extract_fuel_prices(data_path=tmp_path, update_data=False)
        
        # Verificar que NO se ejecutó el script
        mock_run_script.assert_not_called()
        assert len(result) == 1
    
    def test_extract_fuel_prices_no_files(self, tmp_path):
        """Test que levanta error cuando no hay archivos CSV."""
        with pytest.raises(FileNotFoundError, match="No se encontraron archivos CSV"):
            extract_fuel_prices(data_path=tmp_path, update_data=False)
    
    @patch("fuel_price.extract.run_download_script")
    def test_extract_fuel_prices_combines_multiple_files(self, mock_run_script, tmp_path):
        """Test que combina múltiples archivos CSV correctamente."""
        # Crear múltiples archivos
        for i in range(3):
            df = pd.DataFrame({
                "fecha": [f"2024-0{i+1}-01"],
                "precio": [100 + i]
            })
            df.to_csv(tmp_path / f"precios_202{i+2}.csv", index=False)
        
        result = extract_fuel_prices(data_path=tmp_path, update_data=True)
        
        assert len(result) == 3
        # Verificar que contiene los valores esperados (sin importar orden)
        assert set(result["precio"].tolist()) == {100, 101, 102}


# ============================================================================
# Tests para extract_dolar_bluelytics
# ============================================================================

class TestExtractDolarBluelytics:
    """Tests para la función extract_dolar_bluelytics."""
    
    @patch("fuel_price.extract.requests.get")
    def test_extract_dolar_bluelytics_success(self, mock_get, tmp_path):
        """Test extracción exitosa de cotización USD/ARS."""
        # Mock de respuesta de API - usando fechas simples sin timezone
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "date": "2024-01-01",
                "source": "Oficial",
                "value_buy": 800.0,
                "value_sell": 900.0
            },
            {
                "date": "2024-01-01",
                "source": "Blue",
                "value_buy": 950.0,
                "value_sell": 1050.0
            },
            {
                "date": "2024-01-02",
                "source": "Oficial",
                "value_buy": 810.0,
                "value_sell": 910.0
            },
            {
                "date": "2024-01-02",
                "source": "Blue",
                "value_buy": 960.0,
                "value_sell": 1060.0
            }
        ]
        mock_get.return_value = mock_response
        
        # Ejecutar
        result = extract_dolar_bluelytics(
            start_date="2024-01-01",
            end_date="2024-01-02",
            tipos=['oficial', 'blue'],
            output_path=tmp_path
        )
        
        # Verificaciones
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "fecha" in result.columns
        assert "usd_ars_oficial" in result.columns
        assert "usd_ars_blue" in result.columns
        assert "brecha_cambiaria_pct" in result.columns
        
        # Verificar que se guardó el archivo
        saved_file = tmp_path / "usd_ars_bluelytics.csv"
        assert saved_file.exists()
        
        # Verificar llamada a API
        mock_get.assert_called_once_with(
            "https://api.bluelytics.com.ar/v2/evolution.json",
            timeout=30
        )
    
    @patch("fuel_price.extract.requests.get")
    def test_extract_dolar_bluelytics_only_oficial(self, mock_get, tmp_path):
        """Test extracción solo con tipo oficial."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "date": "2024-01-01",
                "source": "Oficial",
                "value_buy": 800.0,
                "value_sell": 900.0
            }
        ]
        mock_get.return_value = mock_response
        
        result = extract_dolar_bluelytics(
            start_date="2024-01-01",
            end_date="2024-01-01",
            tipos=['oficial'],
            output_path=tmp_path
        )
        
        assert "usd_ars_oficial" in result.columns
        assert "usd_ars_blue" not in result.columns
    
    @patch("fuel_price.extract.requests.get")
    def test_extract_dolar_bluelytics_api_error(self, mock_get, tmp_path):
        """Test que levanta error cuando falla la API."""
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        with pytest.raises(ValueError, match="Error al obtener datos de Bluelytics"):
            extract_dolar_bluelytics(
                start_date="2024-01-01",
                end_date="2024-01-01",
                output_path=tmp_path
            )
    
    @patch("fuel_price.extract.requests.get")
    @patch("fuel_price.extract.get_today_date")
    def test_extract_dolar_bluelytics_with_defaults(self, mock_today, mock_get, tmp_path):
        """Test que usa fecha de hoy por defecto."""
        mock_today.return_value = "2024-12-31"
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "date": "2024-01-01",
                "source": "Oficial",
                "value_buy": 800.0,
                "value_sell": 900.0
            }
        ]
        mock_get.return_value = mock_response
        
        with patch("fuel_price.extract.get_default_data_path", return_value=tmp_path):
            result = extract_dolar_bluelytics(start_date="2024-01-01")
        
        # Verificar que procesó correctamente
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


# ============================================================================
# Tests para extract_all_data
# ============================================================================

class TestExtractAllData:
    """Tests para la función extract_all_data."""
    
    @patch("fuel_price.extract.extract_dolar_bluelytics")
    @patch("fuel_price.extract.extract_fuel_prices")
    @patch("fuel_price.extract.extract_brent_prices")
    def test_extract_all_data_success(self, mock_brent, mock_fuel, mock_dolar):
        """Test extracción completa de todas las fuentes."""
        # Mock de datos
        brent_df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "brent_price_usd": [80.5, 81.2]
        })
        fuel_df = pd.DataFrame({
            "fecha": ["2024-01-01"],
            "precio": [100]
        })
        dolar_df = pd.DataFrame({
            "fecha": pd.to_datetime(["2024-01-01"]),
            "usd_ars_oficial": [850.0],
            "usd_ars_blue": [1000.0],
            "brecha_cambiaria_pct": [17.65]
        })
        
        mock_brent.return_value = brent_df
        mock_fuel.return_value = fuel_df
        mock_dolar.return_value = dolar_df
        
        # Ejecutar
        result_brent, result_fuel, result_dolar = extract_all_data()
        
        # Verificaciones
        assert isinstance(result_brent, pd.DataFrame)
        assert isinstance(result_fuel, pd.DataFrame)
        assert isinstance(result_dolar, pd.DataFrame)
        assert len(result_brent) == 2
        assert len(result_fuel) == 1
        assert len(result_dolar) == 1
        
        # Verificar que se llamaron las tres funciones
        mock_brent.assert_called_once()
        mock_fuel.assert_called_once()
        mock_dolar.assert_called_once()
    
    @patch("fuel_price.extract.extract_dolar_bluelytics")
    @patch("fuel_price.extract.extract_fuel_prices")
    @patch("fuel_price.extract.extract_brent_prices")
    def test_extract_all_data_with_custom_parameters(self, mock_brent, mock_fuel, mock_dolar, tmp_path):
        """Test con parámetros personalizados."""
        mock_brent.return_value = pd.DataFrame({"date": ["2024-01-01"], "brent_price_usd": [80.5]})
        mock_fuel.return_value = pd.DataFrame({"fecha": ["2024-01-01"], "precio": [100]})
        mock_dolar.return_value = pd.DataFrame({
            "fecha": pd.to_datetime(["2024-01-01"]),
            "usd_ars_oficial": [850.0]
        })
        
        extract_all_data(
            brent_start_date="2023-01-01",
            brent_end_date="2024-12-31",
            fuel_data_path=tmp_path,
            update_all=False
        )
        
        # Verificar que se pasaron los parámetros correctos
        mock_brent.assert_called_once_with(
            start_date="2023-01-01",
            end_date="2024-12-31",
            output_path=tmp_path
        )
        mock_fuel.assert_called_once_with(
            data_path=tmp_path,
            update_data=False
        )
        mock_dolar.assert_called_once_with(
            start_date="2023-01-01",
            end_date="2024-12-31",
            tipos=['oficial', 'blue'],
            output_path=tmp_path
        )
    
    @patch("fuel_price.extract.extract_dolar_bluelytics")
    @patch("fuel_price.extract.extract_fuel_prices")
    @patch("fuel_price.extract.extract_brent_prices")
    @patch("fuel_price.extract.get_today_date")
    def test_extract_all_data_prints_summary(self, mock_date, mock_brent, mock_fuel, mock_dolar, capsys):
        """Test que imprime resumen correcto."""
        mock_date.return_value = "2024-11-01"
        mock_brent.return_value = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "brent_price_usd": [80.5, 81.2]
        })
        mock_fuel.return_value = pd.DataFrame({
            "fecha": ["2024-01-01"],
            "precio": [100]
        })
        mock_dolar.return_value = pd.DataFrame({
            "fecha": pd.to_datetime(["2024-01-01"]),
            "usd_ars_oficial": [850.0],
            "usd_ars_blue": [1000.0],
            "brecha_cambiaria_pct": [17.65]
        })
        
        extract_all_data()
        
        captured = capsys.readouterr()
        assert "EXTRACCIÓN COMPLETA" in captured.out
        assert "EXTRACCIÓN COMPLETADA EXITOSAMENTE" in captured.out
        assert "2024-11-01" in captured.out
        assert "Registros: 2" in captured.out  # Brent
        assert "Registros: 1" in captured.out  # Fuel y Dolar