"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: An ETL process to fill the COOP-DA-Database with initial sample
data from the MyAnimeList API. This process is meant to be invoked once,
but it is idempotent and can be run multiple times while still yielding
the same data and structure.
"""
import logging
import pandas as pd
import psycopg2
from dotenv import dotenv_values
from DBToolBox.DataConnectors import (
    insert_db, connect_db, get_alchemy_engine_db
)
from sql import (
    create_schema, create_dim_day, add_metadata, analyze_column_stats
)


if __name__ == '__main__':
    
    config = dotenv_values(".env")
    logging.basicConfig(
        level=logging.INFO,
        filename="coop-da-etl.log",
        format="%(asctime)s:%(levelname)s:%(message)s",
    )
    logging.info("Beginning ETL Process...")
    print("Beginning ETL Process...")
    # Get raw data
    all_anime = pd.read_csv(
        r'/src/python-env/data/all_anime.csv', on_bad_lines='skip'
    )
    anime_stats = pd.read_csv(
        r'/src/python-env/data/anime_stats.csv', on_bad_lines='skip'
    )
    anime_scores = pd.read_csv(
        r'/src/python-env/data/anime_scores.csv', on_bad_lines='skip'
    )
    # Clean the data
    logging.info("Cleaning and transforming raw data")
    print("Cleaning and transforming raw data")
    # Rename columns
    all_anime.rename(columns={"id": "anime_id", "title": "anime_title",}, inplace=True)
    # Format dates
    all_anime['load_date'] = pd.to_datetime(all_anime['load_date']).dt.normalize()
    all_anime['aired_from'] = pd.to_datetime(all_anime['aired_from']).dt.normalize()
    all_anime['aired_to'] = pd.to_datetime(all_anime['aired_to']).dt.normalize()
    anime_stats['load_date'] = pd.to_datetime(anime_stats['load_date']).dt.normalize()
    anime_scores['load_date'] = pd.to_datetime(anime_scores['load_date']).dt.normalize()
    # Drop duplicates
    all_anime.drop_duplicates(subset='anime_id', keep='last', inplace=True)
    anime_scores.drop_duplicates(subset=['anime_id', 'score'], keep='last', inplace=True)
    anime_stats.drop_duplicates(subset='anime_id', keep='last', inplace=True)
    # Transform the data
    # Separate raw scores from percentages
    votes_cols = [
        'anime_id', 'load_date', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ]
    # Pivot on scores (1-10)
    anime_votes_raw = anime_scores.pivot_table(
        index='anime_id', columns='score', values='votes'
    )
    anime_votes_raw.index.names = ['id']
    anime_votes_raw['anime_id'] = anime_votes_raw.index
    anime_votes_raw['load_date'] = anime_scores['load_date']
    anime_votes_raw['load_date'].fillna(method='backfill', inplace=True)
    anime_votes_raw = anime_votes_raw[votes_cols]
    anime_votes_pct = anime_scores.pivot_table(
    index='anime_id', columns='score', values='percentage'
    )
    anime_votes_pct.index.names = ['id']
    anime_votes_pct['anime_id'] = anime_votes_pct.index
    anime_votes_pct['load_date'] = anime_scores['load_date']
    anime_votes_pct['load_date'].fillna(method='backfill', inplace=True)
    anime_votes_pct = anime_votes_pct[votes_cols]
    # Create tables with stats and scores merged
    anime_stats_scores_cols = [
        'anime_id', 'load_date','anime_title', 'status', 
        'rating', 'score', 'favorites',  'airing', 'aired_from', 'aired_to',
        'watching', 'completed', 'on_hold', 'dropped', 'plan_to_watch', 'total',
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ]
    anime_stats_and_scores_raw = all_anime.merge(anime_votes_raw, how='inner', on='anime_id')
    anime_stats_and_scores_raw = anime_stats_and_scores_raw.merge(anime_stats, how='inner', on='anime_id')
    anime_stats_and_scores_raw['load_date'] = anime_stats_and_scores_raw['load_date_x']
    anime_stats_and_scores_raw = anime_stats_and_scores_raw[anime_stats_scores_cols]
    anime_stats_and_scores_pct = all_anime.merge(anime_votes_pct, how='inner', on='anime_id')
    anime_stats_and_scores_pct = anime_stats_and_scores_pct.merge(anime_stats, how='inner', on='anime_id')
    anime_stats_and_scores_pct['load_date'] = anime_stats_and_scores_pct['load_date_x']
    anime_stats_and_scores_pct = anime_stats_and_scores_pct[anime_stats_scores_cols]
    # Load to database
    try:
        engine = get_alchemy_engine_db()
        logging.info("Creating Schemas...")
        print("Creating Schemas...")
        with engine.connect() as conn:
        # Create schema
            conn.execute(create_schema)
            conn.execute(create_dim_day)
            conn.execute("CREATE EXTENSION IF NOT EXISTS tablefunc;")
        # Insert raw data
        logging.info("Inserting data into database...")
        print("Inserting data into database...")
        # Raw data
        insert_db(all_anime, 'all_anime', 'anime', engine=engine, if_exists='replace')
        insert_db(anime_scores, 'scores', 'anime', engine=engine, if_exists='replace')
        insert_db(anime_stats, 'stats', 'anime', engine=engine, if_exists='replace')
        # Transformed data
        insert_db(
            anime_stats_and_scores_raw, 
            table = 'anime_stats_and_scores_raw',
            schema = 'anime',
            engine = engine, 
            if_exists = 'replace'
        )
        insert_db(
            anime_stats_and_scores_pct, 
            table = 'anime_stats_and_scores_pct',
            schema = 'anime',
            engine = engine, 
            if_exists = 'replace'
        )
        # Add metadata and analyze column statistics
        logging.info("Analyzing column statistics and adding definitions...")
        print("Analyzing column statistics and adding definitions... almost done...")
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(add_metadata)
                cur.execute(analyze_column_stats)
        logging.info(
            "Process complete! Data has been successfully loaded to database!"
        )
        print(
            "Process complete! Data has been successfully loaded to database!"
        )
    except psycopg2.DatabaseError as err:
        logging.exception("A database error occurred")
        print("A database error occurred")
        raise
    finally:
        engine.dispose() # Close any remaining connections
        