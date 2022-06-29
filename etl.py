"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: A collection of functions to perform the ETL process
              for the initial COOP Data Analytics DB Environment
"""
from concurrent.futures import ThreadPoolExecutor
from DBToolBox.DataConnectors import insert_db
import pandas as pd


# Extract
def ingest_raw_data(paths: list) -> list:
    """Returns a DataFrame for each of the valid given paths"""
    data = []
    for f in paths:
        try:
            df = pd.read_csv(f, on_bad_lines="skip")
            data.append(df)
        except:
            print(f"Missing {f}")
    return data


# Transform
def rename_cols(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """Takes in a DataFrame and returns a copy with the columns renamed"""
    return df.rename(columns=cols)


def format_dates(df: pd.DataFrame, datecols: list) -> pd.DataFrame:
    """
    Takes in a DataFrame and returns a copy with the given date columns
    formatted in YYYY-MM-DD format
    """
    for dt_ in datecols:
        df[dt_] = pd.to_datetime(df[dt_]).dt.normalize()
    return df


def remove_duplicates(
    df: pd.DataFrame, subset: list, keep: str = "last"
) -> pd.DataFrame:
    """
    Takes in a DataFrame and returns a copy with duplicates
    removed according to the given subset of keys
    """
    return df.drop_duplicates(subset=subset, keep=keep)


def convert_column(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Returns a copy of the DataFrame with columns converted according
    to the provided configuration file
    """
    for col in config:
        df[col] = df[col].map(config[col])
    return df


def clean_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Returns a DataFrame with duplicates removed and date
    columns formatted as YYYY-MM-DD
    """
    # Rename columns
    if "rename" in config:
        df = rename_cols(df, config["rename"])
    # Convert column datatypes
    if "conversion" in config:
        df = convert_column(df, config["conversion"])
    # Format dates
    if "datecols" in config:
        df = format_dates(df, config["datecols"])
    # Drop duplicates
    if "dupe_index" in config:
        df = remove_duplicates(df, config["dupe-index"])
    return df


def pivot_data(df: pd.DataFrame, config: dict, table: str) -> pd.DataFrame:
    """
    Returns a pivoted version of the given DataFrame based
    on supplied configuration for the given table
    """
    return df.pivot_table(
        index=config[table]["index"],
        columns=config[table]["pivot-on"],
        values=config[table]["values"],
    )


def format_pivot_data(df: pd.DataFrame, config: dict, table: str) -> pd.DataFrame:
    """Returns a formatted version of the given pivoted DataFrame based on provided config"""
    df.index.names = ["id"]
    df["anime_id"] = df.index
    df["load_date"] = config[table]["load-date"]
    df["load_date"].fillna(method="backfill", inplace=True)
    df = df[config[table]["columns"]]
    return df


def create_pivot_table(df: pd.DataFrame, config: dict, table: str) -> pd.DataFrame:
    """
    Takes the given DataFrame and returns a pivoted and formatted version
    based on the provided configuration
    """
    result = pivot_data(df, config, table)
    result = format_pivot_data(result, config, table)
    return result


def join_data(
    config: dict, df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame
) -> pd.DataFrame:
    """
    Takes in three DataFrames in a list and returns the inner join of the three datasets.
    The order that @param data comes in matters. It is expected that the DataFrames are
    provided in the order that you intend to join them:
    (e.g. data[0].merge(data[1] -> .merge(data[2]))
    """
    results = df1.merge(df2, how="inner", on=config["anime-stats-scores"]["primarykey"])
    results = results.merge(
        df3, how="inner", on=config["anime-stats-scores"]["primarykey"]
    )
    results["load_date"] = results["load_date_x"]
    results = results[config["anime-stats-scores"]["columns"]]
    return results


# Load
def execute_sql(conn, queries: list):
    """Executes the given queries on the provided connection"""
    for query in queries:
        conn.execute(query)


def load_data_sync(data: dict, config: dict, engine):
    """Synchronously oads data into the database based on the provided config"""
    for df in data:
        insert_db(
            data[df],
            config[df]["tablename"],
            config[df]["schema"],
            engine=engine,
            if_exists="replace",
        )


def load_data_single(data: tuple, engine, config):
    """Helper function to unpack and load data into database"""
    # unpack data
    table, df = data
    tablename = config[table]["tablename"]
    schema = config[table]["schema"]
    insert_db(df, tablename, schema, engine=engine, if_exists="replace")


def load_data_concurrent(config: dict, data: list, engine):
    """Concurrently loads data into the database based on provided config"""
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda p: load_data_single(p, engine, config), data.items())
