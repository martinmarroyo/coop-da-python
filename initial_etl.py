"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: An ETL process to fill the COOP-DA-Database with initial sample
data from the MyAnimeList API. This process is meant to be invoked once,
but it is idempotent and can be run multiple times while still yielding
the same data and structure.
"""
import logging
import yaml
from yaml.loader import SafeLoader
import psycopg2
from DBToolBox.DataConnectors import get_alchemy_engine_db
import sql
import etl


def main():
    """The main/driver method for the ETL process"""
    # Load our configuration parameters and table definitions
    with open(r"tables.yml", "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    logging.basicConfig(
        level=logging.INFO,
        filename="coop-da-etl.log",
        format="%(asctime)s:%(levelname)s:%(message)s",
    )
    logging.info("Beginning ETL Process...")
    print("Beginning ETL Process...")
    # Get raw data
    all_anime, anime_stats, anime_scores = etl.ingest_raw_data(config["raw-data-loc"])
    # Clean the data
    logging.info("Cleaning and transforming raw data")
    print("Cleaning and transforming raw data")
    all_anime = etl.clean_data(all_anime, config["all-anime"])
    anime_scores = etl.clean_data(anime_scores, config["anime-scores"])
    anime_stats = etl.clean_data(anime_stats, config["anime-stats"])
    # Transform the data
    anime_votes_raw = etl.create_pivot_table(anime_scores, config, "anime-votes-raw")
    anime_votes_pct = etl.create_pivot_table(anime_scores, config, "anime-votes-pct")
    # Create tables with stats and scores merged
    anime_stats_and_scores_raw = etl.join_data(
        config, all_anime, anime_votes_raw, anime_stats
    )
    anime_stats_and_scores_pct = etl.join_data(
        config, all_anime, anime_votes_pct, anime_stats
    )
    # Load to database
    try:
        engine = get_alchemy_engine_db()
        logging.info("Creating Schemas...")
        print("Creating Schemas...")
        with engine.connect() as conn:
            # Create schema
            etl.execute_sql(
                conn,
                [
                    sql.CREATE_SCHEMA,
                    sql.CREATE_DIM_DAY,
                    "CREATE EXTENSION IF NOT EXISTS tablefunc;",
                ],
            )
        # Insert raw data
        logging.info("Inserting data into database...")
        print("Inserting data into database...")
        # Prep DataFrames for insertion
        data = {
            "all-anime": all_anime,
            "anime-scores": anime_scores,
            "anime-stats": anime_stats,
            "anime-votes-raw": anime_votes_raw,
            "anime-votes-pct": anime_votes_pct,
            "anime-stats-scores-raw": anime_stats_and_scores_raw,
            "anime-stats-scores-pct": anime_stats_and_scores_pct,
        }
        etl.load_data_concurrent(config, data, engine)
        # Add metadata and analyze column statistics
        logging.info("Analyzing column statistics and adding definitions...")
        print("Analyzing column statistics and adding definitions... almost done...")
        with engine.connect() as conn:
            etl.execute_sql(conn, [sql.ADD_METADATA, sql.ANALYZE_COLUMN_STATS])
        logging.info("Process complete! Data has been successfully loaded to database!")
        print("Process complete! Data has been successfully loaded to database!")
    except psycopg2.DatabaseError as err:
        logging.exception("A database error occurred")
        print("A database error occurred", err)
        raise
    finally:
        engine.dispose()  # Close any remaining connections


if __name__ == "__main__":
    main()
