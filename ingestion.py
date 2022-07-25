"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: A collection of functions to ingest data as part of
              the ETL process for COOP Data Analytics DataLab
"""
import pandas as pd


def ingest(conf: dict, key: str = "data", val: str = "source") -> dict:
    """Ingests data from location in conf and returns a dictionary of DataFrames"""
    if not conf:
        print("Configuration is empty. Please try again")
        raise KeyError
    data = {}
    tables = conf[key].keys()
    for table in tables:
        try:
            path = conf[key][table][val]
            data[table] = pd.read_csv(path, on_bad_lines="skip")
        except:
            print(f"{table}: Path not found")
            continue
    return data


def rename_cols(data: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """
    Takes in a DataFrame and a dictionary of mapped names returning
    a DataFrame with the new column names from the mapping"""
    result = data.copy().rename(columns=cols)
    return result


def format_dates(data: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Takes in a DataFrame and a list of date columns and returns
    a copy of the DataFrame with those columns converted to YYYY-MM-DD
    format
    """
    result = data.copy()
    for col in cols:
        result[col] = pd.to_datetime(result[col]).dt.normalize()
    return result


def make_dtype_conversions(data: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """
    Takes in a Dataframe and a mapping of columns to desired data types
    and returns a DataFrame with those columns converted based on the mapping
    """
    result = data.copy()
    return result.astype(cols)


def drop_null_rows(data: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a DataFrame and returns a version with null rows removed
    """
    result = data.copy()
    return result.dropna(axis=0, how="all")


def clean(
    data: pd.DataFrame, table: str, conf: dict, key: str = "data"
) -> pd.DataFrame:
    """
    Takes in a DataFrame and cleans it based on the provided configuration, returning
    a cleaned version of the original
    """
    result = data.copy()
    # Format dates
    if conf[key][table].get("date_cols"):
        result = format_dates(result, conf[key][table]["date_cols"])
    # Rename columns
    if conf[key][table].get("rename"):
        result = rename_cols(result, conf[key][table]["rename"])
    # Make type conversions
    if conf[key][table].get("conversion"):
        result = make_dtype_conversions(result, conf[key][table]["conversion"])
    # Drop null rows
    result = drop_null_rows(result)
    return result


def clean_data(data: dict, conf: dict, key: str = "data") -> dict:
    """
    Takes in an dictionary with data/metadata, cleans the data
    based on the provided configuration, and returns a dictionary
    with the same structure but cleaned versions of the data
    """
    cleaned_data = {}
    # Get the table names that we want to clean
    tables = data.keys()
    # Clean each table as specified in configuration
    for table in tables:
        # Get raw data
        raw_data = data[table]
        # Clean it
        raw_data = clean(raw_data, table, conf)
        # Add to result
        cleaned_data[table] = raw_data
    return cleaned_data


def ingest_data(conf: dict) -> dict:
    """
    Ingests and cleans raw data, returning a dictionary
    of the cleaned data which can be accessed by using
    table names as keys
    """
    raw_data = ingest(conf)
    cleaned_data = clean_data(raw_data, conf)
    return cleaned_data
