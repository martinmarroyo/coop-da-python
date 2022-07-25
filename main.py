"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: An ETL process to fill the COOP-DA-Database with initial sample
data from the MyAnimeList API. This process is meant to be invoked once,
but it is idempotent and can be run multiple times while still yielding
the same data and structure.
"""
import sys
import logging
import ingestion
import transform
import writer
import sql
import yaml
import psycopg2
from yaml.loader import SafeLoader
from DBToolBox.DBConnector import DataConnector


def main():
    """The main/driver method for the ETL process"""
    # Load our configuration parameters and table definitions
    with open(r"tables.yml", "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    logging.basicConfig(
        level=logging.INFO,
        filename="coop-da-etl.log",
        format="%(asctime)s:%(levelname)s:%(message)s",
        handlers=[
            logging.FileHandler("coop-da-etl.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Beginning ETL Process...")
    logging.info("Cleaning and transforming raw data")
    # Get raw data & clean it
    cleaned_data = ingestion.ingest_data(config)
    # Create Data Warehouse
    dwh = transform.create_warehouse(cleaned_data, config)
    # Load to database
    try:
        dc = DataConnector()
        engine = dc.engine
        logging.info("Creating Schemas...")
        with engine.connect() as conn:
            # Create schema
            queries = [sql.CREATE_SCHEMA, sql.CREATE_DIM_DAY, sql.ENABLE_CROSSTAB]
            writer.execute_sql(conn, queries)
        # Insert raw data
        logging.info("Inserting data into database...")
        writer.load_data_concurrent(config, cleaned_data, engine)
        logging.info("Creating Data Warehouse...")
        writer.load_data_concurrent(config, dwh, engine)
        # Add metadata and analyze column statistics
        logging.info("Analyzing column statistics and adding metadata...")
        with engine.connect() as conn:
            # conn.execute(sql.ADD_METADATA)
            conn.execute(sql.ANALYZE_COLUMN_STATS)
        logging.info("Process complete! Data has been successfully loaded to database!")
    except psycopg2.DatabaseError:
        logging.exception("A database error occurred")
        raise
    finally:
        engine.dispose()  # Close any remaining connections


if __name__ == "__main__":
    main()
