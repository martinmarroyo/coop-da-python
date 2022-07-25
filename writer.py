"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: A collection of functions to load data for the
              COOP Data Analytics DataLab
"""
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


def execute_sql(conn, queries: list):
    """Executes the given queries on the provided connection"""
    for query in queries:
        conn.execute(query)


def load_data_sync(data: dict, conf: dict, engine):
    """Synchronously loads data into the database based on the provided conf"""
    for df in data:
        data[df].to_sql(
            conf[df]["tablename"],
            engine,
            schema=conf[df]["schema"],
            if_exists="replace",
            method="multi",
            chunksize=1000,
            index=False,
        )


def load_data_single(data: tuple, engine, conf: dict, key: str = "data"):
    """Helper function to unpack and load data into database"""
    # unpack data
    table, df = data
    tablename = conf[key][table]["tablename"]
    schema = conf[table]["schema"]
    df.to_sql(
        tablename,
        engine,
        schema=schema,
        if_exists="replace",
        method="multi",
        chunksize=1000,
        index=False,
    )


def load_data_concurrent(conf: dict, data: dict, engine):
    """Concurrently loads data into the database based on provided conf"""
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda p: load_data_single(p, engine, conf), data.items())
