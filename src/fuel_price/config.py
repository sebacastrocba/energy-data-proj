import numpy as np

# Extract config
START_DATE_BRENT = "2024-01-01"
END_DATE_BRENT = None

# Mapping de nombres de productos a nombres estandarizados SE
PRODUCTO_MAP = {
    "nafta (super) entre 92 y 95 ron": "NAFTA GRADO 2",
    "nafta (premium) de más de 95 ron": "NAFTA GRADO 3",
    "nafta (común) hasta 92 ron": "NAFTA GRADO 1",
    "gas oil grado 2": "GASOIL GRADO 2",
    "gas oil grado 3": "GASOIL GRADO 3",
    "gnc": "GNC",
    "kerosene": "KEROSENE",
    "glpa": "GLPA",
    "n/d": np.nan,
}

# Lista de columnas relevantes SE
COLUMNAS_RELEVANTES = [
    "periodo",
    "provincia",
    "bandera",
    "producto",
    "precio_surtidor",
    "volumen",
]
