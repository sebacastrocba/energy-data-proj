# Tests para funciones de carga a PostgreSQL

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, date
import psycopg2

from fuel_price.load import (
    # Conexión
    get_postgres_connection,
    test_connection,
    # Carga a Staging
    load_brent_to_staging,
    load_fuel_to_staging,
    load_usd_ars_to_staging,
    # Carga a Analytics
    load_brent_to_analytics,
    load_fuel_to_analytics,
    load_usd_ars_to_analytics,
    # Pipeline completo
    load_all_data,
)


# ============================================================================
# Tests para funciones de CONEXIÓN
# ============================================================================


class TestConnectionFunctions:
    """Tests para funciones de conexión a PostgreSQL."""

    @patch("fuel_price.load.psycopg2.connect")
    @patch("fuel_price.load.os.getenv")
    def test_get_postgres_connection_with_defaults(self, mock_getenv, mock_connect):
        """Test que get_postgres_connection usa valores por defecto de .env."""
        # Configurar mocks
        mock_getenv.side_effect = lambda key, default: {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "fuel_prices_db",
            "POSTGRES_USER": "fuel_user",
            "POSTGRES_PASSWORD": "fuel_password",
        }.get(key, default)

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Ejecutar
        conn = get_postgres_connection()

        # Verificar
        assert conn == mock_conn
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            database="fuel_prices_db",
            user="fuel_user",
            password="fuel_password",
        )

    @patch("fuel_price.load.psycopg2.connect")
    def test_get_postgres_connection_with_custom_params(self, mock_connect):
        """Test que get_postgres_connection acepta parámetros personalizados."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Ejecutar con parámetros custom
        conn = get_postgres_connection(
            host="custom_host",
            port=5433,
            database="custom_db",
            user="custom_user",
            password="custom_pass",
        )

        # Verificar
        assert conn == mock_conn
        mock_connect.assert_called_once_with(
            host="custom_host",
            port=5433,
            database="custom_db",
            user="custom_user",
            password="custom_pass",
        )

    @patch("fuel_price.load.psycopg2.connect")
    def test_get_postgres_connection_raises_on_error(self, mock_connect):
        """Test que get_postgres_connection levanta error si falla la conexión."""
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        with pytest.raises(psycopg2.Error):
            get_postgres_connection()

    @patch("fuel_price.load.get_postgres_connection")
    def test_test_connection_success(self, mock_get_conn):
        """Test que test_connection retorna True cuando la conexión es exitosa."""
        # Mock de conexión y cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("PostgreSQL 13.0",)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_get_conn.return_value = mock_conn

        # Ejecutar
        result = test_connection()

        # Verificar
        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT version();")
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("fuel_price.load.get_postgres_connection")
    def test_test_connection_failure(self, mock_get_conn):
        """Test que test_connection retorna False cuando falla."""
        mock_get_conn.side_effect = Exception("Connection error")

        # Ejecutar
        result = test_connection()

        # Verificar
        assert result is False


# ============================================================================
# Tests para carga a STAGING
# ============================================================================


class TestLoadToStaging:
    """Tests para funciones de carga a staging."""

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_staging_success(self, mock_get_conn, mock_execute_values):
        """Test carga exitosa de Brent a staging."""
        # Preparar datos
        data = {
            "date": pd.to_datetime(["2022-01-01", "2022-01-02"]),
            "brent_price": [80.5, 81.2],
        }
        df = pd.DataFrame(data)

        # Mock de conexión y cursor
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_brent_to_staging(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        # Verificar que TRUNCATE fue ejecutado
        assert mock_cursor.execute.called
        # Verificar que execute_values fue llamado
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_staging_without_truncate(
        self, mock_get_conn, mock_execute_values
    ):
        """Test carga de Brent sin truncate."""
        data = {"date": pd.to_datetime(["2022-01-01"]), "brent_price": [80.5]}
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar sin truncate
        rows_inserted = load_brent_to_staging(df, truncate=False)

        # Verificar que NO se llamó TRUNCATE
        assert not mock_cursor.execute.called
        # Verificar execute_values fue llamado
        assert mock_execute_values.called
        assert rows_inserted == 1
        mock_conn.commit.assert_called_once()

    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_staging_missing_columns(self, mock_get_conn):
        """Test que levanta error si faltan columnas requeridas."""
        # DataFrame sin columna 'brent_price'
        data = {"date": pd.to_datetime(["2022-01-01"])}
        df = pd.DataFrame(data)

        with pytest.raises(ValueError, match="debe contener columnas"):
            load_brent_to_staging(df)

    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_staging_rollback_on_error(self, mock_get_conn):
        """Test que hace rollback si hay error."""
        data = {"date": pd.to_datetime(["2022-01-01"]), "brent_price": [80.5]}
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Simular error en commit
        mock_conn.commit.side_effect = Exception("Database error")

        # Ejecutar y verificar que levanta error
        with pytest.raises(Exception):
            load_brent_to_staging(df)

        # Verificar rollback
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("fuel_price.load.get_postgres_connection")
    def test_load_fuel_to_staging_success(self, mock_get_conn):
        """Test carga exitosa de combustibles a staging."""
        data = {
            "periodo": pd.to_datetime(["2022-01-01", "2022-02-01"]),
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "producto": ["NAFTA GRADO 2", "GASOIL GRADO 2"],
            "precio_surtidor": [100.0, 105.0],
            "volumen": [1000, 1100],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_fuel_to_staging(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        mock_cursor.execute.assert_called_once()  # TRUNCATE
        mock_conn.commit.assert_called_once()

    @patch("fuel_price.load.get_postgres_connection")
    def test_load_fuel_to_staging_missing_columns(self, mock_get_conn):
        """Test que levanta error si faltan columnas requeridas en fuel."""
        # DataFrame sin columna 'producto'
        data = {
            "periodo": pd.to_datetime(["2022-01-01"]),
            "provincia": ["Buenos Aires"],
            "bandera": ["YPF"],
            "precio_surtidor": [100.0],
            "volumen": [1000],
        }
        df = pd.DataFrame(data)

        with pytest.raises(ValueError, match="debe contener columnas"):
            load_fuel_to_staging(df)

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_usd_ars_to_staging_success(self, mock_get_conn, mock_execute_values):
        """Test carga exitosa de USD/ARS a staging."""
        data = {
            "date": pd.to_datetime(["2022-01-01", "2022-01-02"]),
            "usd_ars_oficial": [100.0, 101.0],
            "usd_ars_blue": [180.0, 182.0],
            "brecha_cambiaria_pct": [80.0, 80.2],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_usd_ars_to_staging(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        assert mock_cursor.execute.called  # TRUNCATE
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_usd_ars_to_staging_without_brecha(
        self, mock_get_conn, mock_execute_values
    ):
        """Test carga de USD/ARS sin columna brecha_cambiaria_pct."""
        data = {
            "date": pd.to_datetime(["2022-01-01"]),
            "usd_ars_oficial": [100.0],
            "usd_ars_blue": [180.0],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_usd_ars_to_staging(df)

        # Verificar que funciona sin la columna opcional
        assert rows_inserted == 1
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()


# ============================================================================
# Tests para carga a ANALYTICS
# ============================================================================


class TestLoadToAnalytics:
    """Tests para funciones de carga a analytics."""

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_analytics_success(self, mock_get_conn, mock_execute_values):
        """Test carga exitosa de Brent a analytics."""
        data = {
            "year": [2022, 2022],
            "month": [1, 2],
            "avg_brent_price_usd": [80.5, 82.0],
            "min_brent_price_usd": [78.0, 80.0],
            "max_brent_price_usd": [83.0, 84.0],
            "record_count": [31, 28],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_brent_to_analytics(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        assert mock_cursor.execute.called  # TRUNCATE
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()

    @patch("fuel_price.load.get_postgres_connection")
    def test_load_brent_to_analytics_missing_columns(self, mock_get_conn):
        """Test que levanta error si faltan columnas en analytics."""
        # DataFrame sin columnas de estadísticas
        data = {"year": [2022], "month": [1]}
        df = pd.DataFrame(data)

        with pytest.raises(ValueError, match="debe contener columnas"):
            load_brent_to_analytics(df)

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_fuel_to_analytics_success(self, mock_get_conn, mock_execute_values):
        """Test carga exitosa de combustibles a analytics."""
        data = {
            "year": [2022, 2022],
            "month": [1, 2],
            "provincia": ["Buenos Aires", "Córdoba"],
            "bandera": ["YPF", "Shell"],
            "producto": ["NAFTA GRADO 2", "GASOIL GRADO 2"],
            "precio_surtidor_mediana": [100.0, 105.0],
            "volumen_total": [50000, 55000],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_fuel_to_analytics(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()

    @patch("fuel_price.load.execute_values")
    @patch("fuel_price.load.get_postgres_connection")
    def test_load_usd_ars_to_analytics_success(
        self, mock_get_conn, mock_execute_values
    ):
        """Test carga exitosa de USD/ARS a analytics."""
        data = {
            "year": [2022, 2022],
            "month": [1, 2],
            "avg_usd_ars_oficial": [100.0, 105.0],
            "avg_usd_ars_blue": [180.0, 190.0],
            "avg_brecha_cambiaria_pct": [80.0, 81.0],
            "record_count": [31, 28],
        }
        df = pd.DataFrame(data)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Ejecutar
        rows_inserted = load_usd_ars_to_analytics(df, truncate=True)

        # Verificar
        assert rows_inserted == 2
        assert mock_execute_values.called
        mock_conn.commit.assert_called_once()


# ============================================================================
# Tests para pipeline completo
# ============================================================================


class TestLoadAllData:
    """Tests para la función de pipeline completo."""

    @patch("fuel_price.load.test_connection")
    @patch("fuel_price.load.load_brent_to_staging")
    @patch("fuel_price.load.load_fuel_to_staging")
    @patch("fuel_price.load.load_usd_ars_to_staging")
    @patch("fuel_price.load.load_brent_to_analytics")
    @patch("fuel_price.load.load_fuel_to_analytics")
    @patch("fuel_price.load.load_usd_ars_to_analytics")
    def test_load_all_data_with_analytics(
        self,
        mock_usd_analytics,
        mock_fuel_analytics,
        mock_brent_analytics,
        mock_usd_staging,
        mock_fuel_staging,
        mock_brent_staging,
        mock_test_conn,
    ):
        """Test de carga completa con analytics."""
        # Configurar mocks
        mock_test_conn.return_value = True
        mock_brent_staging.return_value = 100
        mock_fuel_staging.return_value = 200
        mock_usd_staging.return_value = 50
        mock_brent_analytics.return_value = 10
        mock_fuel_analytics.return_value = 20
        mock_usd_analytics.return_value = 5

        # Preparar DataFrames de prueba
        brent_df = pd.DataFrame(
            {"date": pd.to_datetime(["2022-01-01"]), "brent_price": [80.5]}
        )
        fuel_df = pd.DataFrame(
            {
                "periodo": pd.to_datetime(["2022-01-01"]),
                "provincia": ["Buenos Aires"],
                "bandera": ["YPF"],
                "producto": ["NAFTA GRADO 2"],
                "precio_surtidor": [100.0],
                "volumen": [1000],
            }
        )
        usd_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2022-01-01"]),
                "usd_ars_oficial": [100.0],
                "usd_ars_blue": [180.0],
            }
        )

        # Ejecutar
        load_all_data(brent_df, fuel_df, usd_df, load_to_analytics=True)

        # Verificar que se llamaron todas las funciones
        mock_test_conn.assert_called_once()
        mock_brent_staging.assert_called_once()
        mock_fuel_staging.assert_called_once()
        mock_usd_staging.assert_called_once()
        mock_brent_analytics.assert_called_once()
        mock_fuel_analytics.assert_called_once()
        mock_usd_analytics.assert_called_once()

    @patch("fuel_price.load.test_connection")
    @patch("fuel_price.load.load_brent_to_staging")
    @patch("fuel_price.load.load_fuel_to_staging")
    @patch("fuel_price.load.load_usd_ars_to_staging")
    def test_load_all_data_without_analytics(
        self, mock_usd_staging, mock_fuel_staging, mock_brent_staging, mock_test_conn
    ):
        """Test de carga solo a staging (sin analytics)."""
        # Configurar mocks
        mock_test_conn.return_value = True
        mock_brent_staging.return_value = 100
        mock_fuel_staging.return_value = 200
        mock_usd_staging.return_value = 50

        # Preparar DataFrames de prueba
        brent_df = pd.DataFrame(
            {"date": pd.to_datetime(["2022-01-01"]), "brent_price": [80.5]}
        )
        fuel_df = pd.DataFrame(
            {
                "periodo": pd.to_datetime(["2022-01-01"]),
                "provincia": ["Buenos Aires"],
                "bandera": ["YPF"],
                "producto": ["NAFTA GRADO 2"],
                "precio_surtidor": [100.0],
                "volumen": [1000],
            }
        )
        usd_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2022-01-01"]),
                "usd_ars_oficial": [100.0],
                "usd_ars_blue": [180.0],
            }
        )

        # Ejecutar sin analytics
        load_all_data(brent_df, fuel_df, usd_df, load_to_analytics=False)

        # Verificar que se llamaron solo las funciones de staging
        mock_test_conn.assert_called_once()
        mock_brent_staging.assert_called_once()
        mock_fuel_staging.assert_called_once()
        mock_usd_staging.assert_called_once()

    @patch("fuel_price.load.test_connection")
    def test_load_all_data_connection_error(self, mock_test_conn):
        """Test que levanta error si falla la conexión."""
        mock_test_conn.return_value = False

        brent_df = pd.DataFrame({"date": ["2022-01-01"], "brent_price": [80.5]})
        fuel_df = pd.DataFrame(
            {
                "periodo": ["2022-01-01"],
                "provincia": ["BA"],
                "bandera": ["YPF"],
                "producto": ["NAFTA"],
                "precio_surtidor": [100.0],
                "volumen": [1000],
            }
        )
        usd_df = pd.DataFrame(
            {
                "date": ["2022-01-01"],
                "usd_ars_oficial": [100.0],
                "usd_ars_blue": [180.0],
            }
        )

        # Verificar que levanta ConnectionError
        with pytest.raises(ConnectionError, match="No se puede conectar"):
            load_all_data(brent_df, fuel_df, usd_df)
