from config import *
import pandas as pd

def convert_cols_to_datatime(df, col_names:list, time_delta=False):
    """

    convert multiple dfs columns to datatime 

    paramters 
    ---------
    col_names : "list" of the column names you want to convert to datetime 

    time_delta : 'bool' 'optional' wether you want to convert to time_delta or not. By default, it is false

    returns  
    ---------
    pd.Dataframe 

    """
    df = df.copy()

    for col in col_names:
        if col in df.columns : 
            if time_delta: 
                df[col] = pd.to_timedelta(
                   df[col],
                   errors='coerce',
                )

            else : 
                df[col] = pd.to_datetime(
                    df[col],
                    errors='coerce',
                )
        else : 
            print(f'warning: columns {col} not in the given df')
    return df



def convert_id_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all columns in the DataFrame whose names end with '_id' to strings.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with all '_id' columns converted to strings.
    """
    df = df.copy()
    id_cols = [col for col in df.columns if col.lower().endswith('_id')]

    for col in id_cols:
        df[col] = df[col].astype(str)
    
    return df

def drop_id_cols(df): 
    """
    drops all the columns that ends with '_id' in the given df 

    parameters
    ----------
    df : the dataframe 
    """
    df_copy = df.copy()
    for col in df_copy.columns :
        if col.endswith('_id'):
            df_copy = df_copy.drop(columns=[f'{col}_id'])
     
    return df_copy