# Funciones para transformar datos

import pandas as pd

def clean_brent_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de precios del petróleo Brent.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos originales.

    Returns:
        pd.DataFrame: DataFrame limpio y transformado.
    """

    # Copiar el DataFrame para no modificar el original
    cleaned_df = df.copy()

    # 1. Eliminar filas con valores nulos
    cleaned_df = cleaned_df.dropna()

    # 2. Asegurarse de que la columna 'date' sea de tipo datetime
    cleaned_df['date'] = pd.to_datetime(cleaned_df['date'])

    # 3. Asegurarse de que la columna 'brent_price' sea de tipo float
    cleaned_df['brent_price'] = cleaned_df['brent_price'].astype(float)

    # 4. Eliminar duplicados
    cleaned_df = cleaned_df.drop_duplicates(subset=['date'])

    # 5. Ordenar por fecha
    cleaned_df = cleaned_df.sort_values(by='date').reset_index(drop=True)

    return cleaned_df

# Probar la función
if __name__ == "__main__":
    from pda_project.extract import extract_brent_prices

    raw_df = extract_brent_prices()
    clean_df = clean_brent_price(raw_df)

    print("Datos originales:")
    print(raw_df.head())
    print("Datos limpios:")
    print(clean_df.head())