# Tests para funciones de transformación

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock
from pathlib import Path

from fuel_price.transform import (
    # Brent
    clean_brent_price,
    agg_brent_price,
    agg_brent_price_for_analytics,
    process_brent_price_data,
    # Fuel
    clean_fuel_price,
    fuel_price_aggs,
    fuel_price_aggs_for_analytics,
    process_fuel_data_pipeline,
    # Dollar
    clean_dollar_price,
    dollar_price_aggs,
    dollar_price_aggs_for_analytics,
    process_dolar_price_data,
    # Utilidades
    save_to_parquet
)


# ============================================================================
# Tests para transformaciones de BRENT
# ============================================================================

class TestBrentTransformations:
    """Tests para funciones de transformación de precios del Brent."""
    
    def test_clean_brent_price_removes_nulls(self):
        """Test que clean_brent_price elimina valores nulos."""
        data = {
            "date": ["2022-01-01", "2022-01-02", None, "2022-01-04"],
            "brent_price_usd": [80.5, None, 82.0, 83.5]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_brent_price(df)
        
        # Verificar que no hay nulos
        assert cleaned_df.isnull().sum().sum() == 0
        # Verificar que se eliminaron 2 filas (una con date null, otra con price null)
        assert len(cleaned_df) == 2
    
    def test_clean_brent_price_converts_types(self):
        """Test que clean_brent_price convierte tipos correctamente."""
        data = {
            "date": ["2022-01-01", "2022-01-02"],
            "brent_price_usd": ["80.5", "81.2"]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_brent_price(df)
        
        # Verificar tipos
        assert pd.api.types.is_datetime64_any_dtype(cleaned_df['date'])
        assert pd.api.types.is_float_dtype(cleaned_df['brent_price'])
    
    def test_clean_brent_price_removes_duplicates(self):
        """Test que clean_brent_price elimina duplicados."""
        data = {
            "date": ["2022-01-01", "2022-01-01", "2022-01-02"],
            "brent_price_usd": [80.5, 80.5, 81.2]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_brent_price(df)
        
        # Verificar que se eliminó el duplicado
        assert len(cleaned_df) == 2
        assert cleaned_df['date'].is_unique
    
    def test_clean_brent_price_sorts_by_date(self):
        """Test que clean_brent_price ordena por fecha."""
        data = {
            "date": ["2022-01-03", "2022-01-01", "2022-01-02"],
            "brent_price_usd": [82.0, 80.5, 81.2]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_brent_price(df)
        
        # Verificar orden
        assert cleaned_df['date'].is_monotonic_increasing
        assert cleaned_df.iloc[0]['brent_price'] == 80.5
    
    def test_clean_brent_price_renames_columns(self):
        """Test que clean_brent_price renombra columnas correctamente."""
        data = {
            "Date": ["2022-01-01", "2022-01-02"],
            "brent_price_usd": [80.5, 81.2]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_brent_price(df)
        
        # Verificar nombres
        assert 'date' in cleaned_df.columns
        assert 'Date' not in cleaned_df.columns
        assert 'brent_price' in cleaned_df.columns
        assert 'brent_price_usd' not in cleaned_df.columns
    
    def test_agg_brent_price_monthly(self):
        """Test que agg_brent_price agrega mensualmente."""
        data = {
            "date": pd.date_range("2022-01-01", periods=10, freq='D'),
            "brent_price": [80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, 89.0]
        }
        df = pd.DataFrame(data)
        
        aggregated_df = agg_brent_price(df, freq='M', agg_func=np.mean)
        
        # Verificar que tiene 1 registro (todos en enero)
        assert len(aggregated_df) == 1
        assert 'date' in aggregated_df.columns
        assert 'brent_price' in aggregated_df.columns
        # Verificar promedio
        assert aggregated_df.iloc[0]['brent_price'] == 84.5
    
    def test_agg_brent_price_weekly(self):
        """Test que agg_brent_price agrega semanalmente."""
        data = {
            "date": pd.date_range("2022-01-01", periods=14, freq='D'),
            "brent_price": list(range(80, 94))
        }
        df = pd.DataFrame(data)
        
        aggregated_df = agg_brent_price(df, freq='W', agg_func=np.mean)
        
        # Verificar que tiene al menos 2 semanas
        assert len(aggregated_df) >= 2
    
    def test_agg_brent_price_for_analytics(self):
        """Test que agg_brent_price_for_analytics genera estadísticas."""
        data = {
            "date": ["2022-01-15", "2022-01-16", "2022-02-10"],
            "brent_price": [80.0, 85.0, 90.0]
        }
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        analytics_df = agg_brent_price_for_analytics(df)
        
        # Verificar estructura
        assert 'year' in analytics_df.columns
        assert 'month' in analytics_df.columns
        assert 'avg_brent_price_usd' in analytics_df.columns
        assert 'min_brent_price_usd' in analytics_df.columns
        assert 'max_brent_price_usd' in analytics_df.columns
        assert 'record_count' in analytics_df.columns
        
        # Verificar 2 períodos (enero y febrero)
        assert len(analytics_df) == 2
        
        # Verificar estadísticas de enero
        enero = analytics_df[analytics_df['month'] == 1].iloc[0]
        assert enero['avg_brent_price_usd'] == 82.5  # (80+85)/2
        assert enero['min_brent_price_usd'] == 80.0
        assert enero['max_brent_price_usd'] == 85.0
        assert enero['record_count'] == 2
    
    def test_process_brent_price_data_pipeline(self):
        """Test del pipeline completo de Brent."""
        data = {
            "date": ["2022-01-01", "2022-01-02", "2022-01-03", None],
            "brent_price_usd": [80.5, 81.2, None, 83.0]
        }
        df = pd.DataFrame(data)
        
        result_df = process_brent_price_data(df)
        
        # Verificar que el pipeline limpia y agrega
        assert len(result_df) >= 1
        assert 'date' in result_df.columns
        assert 'brent_price' in result_df.columns
        assert result_df.isnull().sum().sum() == 0


# ============================================================================
# Tests para transformaciones de COMBUSTIBLES
# ============================================================================

class TestFuelTransformations:
    """Tests para funciones de transformación de combustibles."""
    
    def test_clean_fuel_price_normalizes_columns(self):
        """Test que clean_fuel_price normaliza nombres de columnas."""
        data = {
            "Periodo": ["2022/01", "2022/02"],
            "Precio Surtidor": [100.0, 105.0],
            "Producto": ["Nafta (super) entre 92 y 95 ron", "Nafta (premium) de más de 95 ron"],
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "volumen": [1000, 1100]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_fuel_price(df)
        
        # Verificar normalización
        assert 'periodo' in cleaned_df.columns
        assert 'precio_surtidor' in cleaned_df.columns
        assert 'producto' in cleaned_df.columns
        assert 'Periodo' not in cleaned_df.columns
    
    def test_clean_fuel_price_converts_types(self):
        """Test que clean_fuel_price convierte tipos correctamente."""
        data = {
            "periodo": ["2022/01", "2022/02"],
            "precio_surtidor": ["100.5", "105.2"],
            "producto": ["Nafta (super) entre 92 y 95 ron", "Nafta (premium) de más de 95 ron"],
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "volumen": [1000, 1100]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_fuel_price(df)
        
        # Verificar tipos
        assert pd.api.types.is_datetime64_any_dtype(cleaned_df['periodo'])
        assert pd.api.types.is_numeric_dtype(cleaned_df['precio_surtidor'])
    
    def test_clean_fuel_price_removes_nulls(self):
        """Test que clean_fuel_price elimina nulos."""
        data = {
            "periodo": ["2022/01", None, "2022/03"],
            "precio_surtidor": [100.0, 105.0, None],
            "producto": ["Nafta (super) entre 92 y 95 ron", "Nafta (premium) de más de 95 ron", "Gas Oil Grado 2"],
            "provincia": ["Buenos Aires", "Córdoba", "Mendoza"],
            "bandera": ["YPF", "Shell", "Axion"],
            "volumen": [1000, 1100, 1200]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_fuel_price(df)
        
        # Verificar que se eliminaron filas con nulos en periodo/precio
        assert len(cleaned_df) == 1
        assert cleaned_df.iloc[0]['producto'] == 'NAFTA GRADO 2'
    
    def test_clean_fuel_price_maps_products(self):
        """Test que clean_fuel_price mapea productos correctamente."""
        data = {
            "periodo": ["2022/01", "2022/02", "2022/03"],
            "precio_surtidor": [100.0, 105.0, 110.0],
            "producto": ["Nafta (super) entre 92 y 95 ron", "Nafta (premium) de más de 95 ron", "Gas Oil Grado 2"],
            "provincia": ["Buenos Aires", "Córdoba", "Mendoza"],
            "bandera": ["YPF", "Shell", "Axion"],
            "volumen": [1000, 1100, 1200]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_fuel_price(df)
        
        # Verificar mapeo
        assert 'NAFTA GRADO 2' in cleaned_df['producto'].values
        assert 'NAFTA GRADO 3' in cleaned_df['producto'].values
        assert 'GASOIL GRADO 2' in cleaned_df['producto'].values
        # Verificar que no quedan nombres originales
        assert 'nafta (super) entre 92 y 95 ron' not in cleaned_df['producto'].values
    
    def test_clean_fuel_price_raises_on_missing_columns(self):
        """Test que clean_fuel_price levanta error si faltan columnas."""
        data = {
            "periodo": ["2022/01"],
            "precio_surtidor": [100.0]
            # Falta 'producto'
        }
        df = pd.DataFrame(data)
        
        with pytest.raises(ValueError, match="Faltan columnas requeridas"):
            clean_fuel_price(df)
    
    def test_fuel_price_aggs_groups_correctly(self):
        """Test que fuel_price_aggs agrupa correctamente."""
        data = {
            "periodo": pd.to_datetime(["2022-01-01", "2022-01-01", "2022-02-01"]),
            "provincia": ["Buenos Aires", "Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell", "YPF"],
            "producto": ["NAFTA GRADO 2", "NAFTA GRADO 2", "NAFTA GRADO 2"],
            "precio_surtidor": [100.0, 105.0, 110.0],
            "volumen": [1000, 1500, 2000]
        }
        df = pd.DataFrame(data)
        
        aggregated_df = fuel_price_aggs(df)
        
        # Verificar estructura
        assert 'precio_surtidor_mediana' in aggregated_df.columns
        assert 'volumen_total' in aggregated_df.columns
        
        # Verificar agregación - 3 grupos diferentes
        assert len(aggregated_df) == 3
    
    def test_fuel_price_aggs_for_analytics(self):
        """Test que fuel_price_aggs_for_analytics extrae year y month."""
        data = {
            "periodo": pd.to_datetime(["2022-01-15", "2022-02-10"]),
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "producto": ["NAFTA GRADO 2", "GASOIL GRADO 2"],
            "precio_surtidor_mediana": [100.0, 110.0],
            "volumen_total": [5000, 6000]
        }
        df = pd.DataFrame(data)
        
        analytics_df = fuel_price_aggs_for_analytics(df)
        
        # Verificar columnas
        assert 'year' in analytics_df.columns
        assert 'month' in analytics_df.columns
        assert 'provincia' in analytics_df.columns
        assert 'bandera' in analytics_df.columns
        assert 'producto' in analytics_df.columns
        
        # Verificar valores
        assert analytics_df.iloc[0]['year'] == 2022
        assert analytics_df.iloc[0]['month'] == 1
        assert analytics_df.iloc[1]['month'] == 2
    
    @patch('fuel_price.transform.save_to_parquet')
    def test_process_fuel_data_pipeline(self, mock_save):
        """Test del pipeline completo de combustibles."""
        data = {
            "periodo": ["2022/01", "2022/02"],
            "precio_surtidor": [100.0, 105.0],
            "producto": ["Nafta (super) entre 92 y 95 ron", "Gas Oil Grado 2"],
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "volumen": [1000, 1100]
        }
        df = pd.DataFrame(data)
        
        result_df = process_fuel_data_pipeline(df, save_staging=False)
        
        # Verificar que el pipeline funciona
        assert len(result_df) >= 1
        assert 'precio_surtidor_mediana' in result_df.columns
        assert 'volumen_total' in result_df.columns
        
        # Verificar que no se llamó save_to_parquet
        mock_save.assert_not_called()


# ============================================================================
# Tests para transformaciones de DÓLAR
# ============================================================================

class TestDollarTransformations:
    """Tests para funciones de transformación de USD/ARS."""
    
    def test_clean_dollar_price_renames_fecha_to_date(self):
        """Test que clean_dollar_price renombra 'fecha' a 'date'."""
        data = {
            "fecha": ["2022-01-01", "2022-01-02"],
            "usd_ars_oficial": [100.0, 101.0],
            "usd_ars_blue": [180.0, 182.0]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_dollar_price(df)
        
        # Verificar renombrado
        assert 'date' in cleaned_df.columns
        assert 'fecha' not in cleaned_df.columns
    
    def test_clean_dollar_price_converts_date_type(self):
        """Test que clean_dollar_price convierte date a datetime."""
        data = {
            "fecha": ["2022-01-01", "2022-01-02"],
            "usd_ars_oficial": [100.0, 101.0],
            "usd_ars_blue": [180.0, 182.0]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_dollar_price(df)
        
        # Verificar tipo
        assert pd.api.types.is_datetime64_any_dtype(cleaned_df['date'])
    
    def test_clean_dollar_price_removes_null_dates(self):
        """Test que clean_dollar_price elimina fechas nulas."""
        data = {
            "fecha": ["2022-01-01", None, "2022-01-03"],
            "usd_ars_oficial": [100.0, 101.0, 102.0],
            "usd_ars_blue": [180.0, 182.0, 184.0]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = clean_dollar_price(df)
        
        # Verificar eliminación
        assert len(cleaned_df) == 2
        assert cleaned_df['date'].notna().all()
    
    def test_dollar_price_aggs_monthly(self):
        """Test que dollar_price_aggs agrega mensualmente."""
        data = {
            "date": pd.date_range("2022-01-01", periods=10, freq='D'),
            "usd_ars_oficial": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
            "usd_ars_blue": [180.0, 181.0, 182.0, 183.0, 184.0, 185.0, 186.0, 187.0, 188.0, 189.0]
        }
        df = pd.DataFrame(data)
        
        aggregated_df = dollar_price_aggs(df, freq='M', agg_func=np.mean)
        
        # Verificar agregación
        assert len(aggregated_df) == 1
        assert 'date' in aggregated_df.columns
        assert 'usd_ars_oficial' in aggregated_df.columns
        assert 'usd_ars_blue' in aggregated_df.columns
        # Verificar promedio
        assert aggregated_df.iloc[0]['usd_ars_oficial'] == 104.5
        assert aggregated_df.iloc[0]['usd_ars_blue'] == 184.5
    
    def test_dollar_price_aggs_for_analytics(self):
        """Test que dollar_price_aggs_for_analytics genera estadísticas."""
        data = {
            "date": pd.to_datetime(["2022-01-15", "2022-01-16", "2022-02-10"]),
            "usd_ars_oficial": [100.0, 102.0, 110.0],
            "usd_ars_blue": [180.0, 184.0, 200.0],
            "brecha_cambiaria_pct": [80.0, 80.4, 81.8]
        }
        df = pd.DataFrame(data)
        
        analytics_df = dollar_price_aggs_for_analytics(df)
        
        # Verificar estructura
        assert 'year' in analytics_df.columns
        assert 'month' in analytics_df.columns
        assert 'avg_usd_ars_oficial' in analytics_df.columns
        assert 'avg_usd_ars_blue' in analytics_df.columns
        assert 'avg_brecha_cambiaria_pct' in analytics_df.columns
        assert 'record_count' in analytics_df.columns
        
        # Verificar 2 períodos
        assert len(analytics_df) == 2
        
        # Verificar estadísticas de enero
        enero = analytics_df[analytics_df['month'] == 1].iloc[0]
        assert enero['avg_usd_ars_oficial'] == 101.0  # (100+102)/2
        assert enero['avg_usd_ars_blue'] == 182.0  # (180+184)/2
        assert enero['record_count'] == 2
    
    def test_process_dolar_price_data_pipeline(self):
        """Test del pipeline completo de dólar."""
        data = {
            "fecha": ["2022-01-01", "2022-01-02", None],
            "usd_ars_oficial": [100.0, 101.0, 102.0],
            "usd_ars_blue": [180.0, 182.0, 184.0]
        }
        df = pd.DataFrame(data)
        
        result_df = process_dolar_price_data(df)
        
        # Verificar que el pipeline funciona
        assert len(result_df) >= 1
        assert 'date' in result_df.columns
        assert 'usd_ars_oficial' in result_df.columns
        assert 'usd_ars_blue' in result_df.columns


# ============================================================================
# Tests para funciones AUXILIARES
# ============================================================================

class TestUtilityFunctions:
    """Tests para funciones auxiliares."""
    
    def test_save_to_parquet(self, tmp_path):
        """Test que save_to_parquet guarda correctamente."""
        data = {
            "date": pd.date_range("2022-01-01", periods=5),
            "value": [1, 2, 3, 4, 5]
        }
        df = pd.DataFrame(data)
        
        file_path = save_to_parquet(df, tmp_path, "test_data")
        
        # Verificar que se creó el archivo
        assert file_path.exists()
        assert file_path.suffix == ".parquet"
        
        # Verificar que se puede leer
        df_read = pd.read_parquet(file_path)
        assert len(df_read) == 5
        assert list(df_read.columns) == ['date', 'value']
    
    def test_save_to_parquet_creates_directory(self, tmp_path):
        """Test que save_to_parquet crea directorios."""
        nested_path = tmp_path / "level1" / "level2"
        
        data = {"value": [1, 2, 3]}
        df = pd.DataFrame(data)
        
        file_path = save_to_parquet(df, nested_path, "test")
        
        # Verificar creación
        assert nested_path.exists()
        assert file_path.exists()
