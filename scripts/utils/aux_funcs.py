import pandas as pd
import os
import glob

# creating a function to save dataframe as parquet file
def save_as_parquet(df: pd.DataFrame, file_path: str) -> None:
    
    """
    Save a pandas DataFrame as a parquet file.

    Parameters:
    df (pd.DataFrame): The DataFrame to save.
    file_path (str): The path where the parquet file will be saved.
    """
    
    df.to_parquet(file_path, index=False)
    
# creating a function to drop NA rows from a dataframe based on the columns, also using strip to remove leading/trailing spaces
def dropna_rows(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    
    """
    Drop rows with missing values from a pandas DataFrame.
    Leading and trailing spaces are removed from the specified columns before checking for emptiness.

    Parameters:
    df (pd.DataFrame): The DataFrame from which to drop rows.
    subset (list): A list of column names to consider for dropping rows.
    """
    
    missing_cols = [col for col in subset if col not in df.columns]
    if missing_cols:
        print(f"Warning: The following columns do not exist in the DataFrame: {missing_cols}")
    # Drop rows with NA values in the specified subset
    df = df[df[subset].notna().all(axis=1)]
    # Drop rows with empty strings (after stripping) in the specified subset
    df = df[df[subset].astype(str).apply(lambda col: col.str.strip() != '').all(axis=1)]
    
    return df.reset_index(drop=True)
    
# creating a function to delete columns from a dataframe
def delete_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    
    """
    Delete specified columns from a pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame from which to delete columns.
    columns (list): A list of column names to delete.

    Returns:
    pd.DataFrame: The DataFrame with specified columns deleted.
    """
    for col in cols:
        if col not in df.columns:
            print(f"Column '{col}' does not exist in the DataFrame.")
    return df.drop(columns=cols, inplace=False, errors='ignore')

# creating a function to treat column dtypes in a dataframe
def dtype_trt(df: pd.DataFrame, cols: list, dtype: dict) -> pd.DataFrame:
    """
    Treat column data types in a pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to treat.
    cols (list): List of column names to treat.
    dtype (dict): Dictionary mapping column names to desired data types.

    Returns:
    pd.DataFrame: The DataFrame with treated column data types.
    
    """
    
    for col in cols:
        if col not in df.columns:
            print(f"Column '{col}' does not exist in the DataFrame.")
            continue
        try: # to category or str
            df[col] = df[col].astype(dtype[col]) if dtype[col] in ['category', 'str'] else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to type '{dtype.get(col, None)}': {e}")
            
        try: # to datetime
            df[col] = pd.to_datetime(df[col], errors='coerce') if dtype[col] == 'datetime' else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to datetime: {e}")
            
        try: # to int
            df[col] = pd.to_numeric(df[col], errors='coerce') if dtype[col] in ['int'] else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to numeric: {e}")
            
        try: # to float
            df[col] = pd.to_numeric(df[col], errors='coerce') if dtype[col] in ['float'] else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to float: {e}")
    
        try: # to boolean
            df[col] = df[col].astype(bool) if dtype[col] == 'bool' else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to boolean: {e}")
        
        try: # to date
            df[col] = df[col].dt.date if dtype[col] == 'date' else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to date: {e}")
            
        try: # to time
            df[col] = df[col].dt.time if dtype[col] == 'time' else df[col]
        except Exception as e:
            print(f"Could not convert column '{col}' to time: {e}")
       
    return df

def standardize_platform(df: pd.DataFrame, column: str, platform_map: dict) -> pd.DataFrame:
    """
    Padroniza a coluna de plataforma usando um dicionário de palavras-chave.
    
    Parameters:
    df (pd.DataFrame): DataFrame com a coluna de plataformas.
    column (str): Nome da coluna a ser padronizada.
    platform_map (dict): Dicionário onde chaves = nome final da plataforma,
                         valores = lista de palavras-chave.
    
    Returns:
    pd.DataFrame: DataFrame com a coluna padronizada.
    """
    # garantir que a coluna exista
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame.")
    
    # padronizar coluna: string, sem espaços, minúscula
    df[column] = df[column].astype(str).str.strip().str.lower()
    
    # criar mapeamento reverso: cada palavra-chave -> nome da plataforma
    reverse_map = {kw.lower(): platform for platform, keywords in platform_map.items() for kw in keywords}
    
    # função de mapeamento
    def map_platform(val):
        val_lower = val.lower()
        for kw, platform in reverse_map.items():
            if kw in val_lower:
                return platform
        return val  # mantém original se não bater
    
    # aplicar mapeamento e capitalizar
    df[column] = df[column].apply(map_platform).str.capitalize()
    
    return df