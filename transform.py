"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: A collection of functions to transform data as part of
              the ETL process for COOP Data Analytics DataLab
"""
import pandas as pd


def normalize_date_partition(data: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Takes in a DataFrame and mapping of columns with date values to 
    replace in order to normalize date partitions from data sources
    
    Example mapping: {column_name: [old_date, new_date]}
    """
    result = data.copy()
    for col in mapping:
        old = mapping[col][0]
        new = mapping[col][1]
        # Get index for partition with old_date
        partition = result.loc[result[col] == old].index
        for part in partition:
            result.loc[part, col] = new
    return result


def create_dim_anime(data: pd.DataFrame, conf: dict, key: str = "data") -> pd.DataFrame:
    """
    Takes in cleaned all_anime data and 
    generates the dim_anime dimension table
    """
    dim_anime = data.copy()
    # Remove avg_score, favorites cols
    dim_anime.drop(["avg_score", "favorites"], axis=1, inplace=True)
    # Add surrogate key
    dim_anime["anime_key"] = range(1000, len(dim_anime.index) + 1000)
    # Add is_current
    dim_anime["is_current"] = dim_anime.duplicated(subset="anime_id", keep="last")
    dim_anime["is_current"] = dim_anime["is_current"].apply(lambda x: not x)
    # Change load_date -> current_as_of
    dim_anime.rename(columns={"load_date": "current_as_of"}, inplace=True)
    # Add expired col
    dim_anime["expired"] = dim_anime.apply(
        # Set the expiration date based on the latest current_as_of for each anime_id
        lambda row: dim_anime[dim_anime["anime_id"] == row["anime_id"]][
            "current_as_of"
        ].max()
        if not row["is_current"]
        else None,
        axis=1,
    )
    # Re-order the columns
    dim_anime = dim_anime[conf[key]["dim_anime"]["columns"]]
    # Normalize the date partitions
    dim_anime = normalize_date_partition(
        dim_anime, {"current_as_of": ["2022-03-12", "2022-03-11"]}
    )
    dim_anime = normalize_date_partition(
        dim_anime, {"current_as_of": ["2022-05-12", "2022-05-11"]}
    )
    return dim_anime


def create_fact_anime_stats(
    cleaned_data: dict, dimension: pd.DataFrame
) -> pd.DataFrame:
    """Creates the fact_anime_stats table based on provided data"""
    # Extract data for enrichment
    enrichment_data = cleaned_data["all_anime"][["anime_id", "avg_score", "favorites"]]
    surrogate_keys = dimension[["anime_id", "anime_key", "current_as_of", "is_current"]]
    anime_stats = cleaned_data["stat"]
    # Normalize dates for anime_stats
    anime_stats = normalize_date_partition(
        anime_stats, {"load_date": ["2022-03-12", "2022-03-11"]}
    )
    anime_stats = normalize_date_partition(
        anime_stats, {"load_date": ["2022-03-21", "2022-03-20"]}
    )
    anime_stats = normalize_date_partition(
        anime_stats, {"load_date": ["2022-05-12", "2022-05-11"]}
    )
    # Enrich anime_stats
    enriched_anime_stats = anime_stats.merge(enrichment_data, how="left", on="anime_id")
    # Backfill to Interpolate missing values where possible. For all other cases, fill with 0s
    enriched_anime_stats["avg_score"] = enriched_anime_stats["avg_score"].fillna(
        method="backfill"
    )
    enriched_anime_stats["favorites"] = enriched_anime_stats["favorites"].fillna(
        method="backfill"
    )
    enriched_anime_stats = enriched_anime_stats.fillna(0)
    # Split data into normalized partitions
    partitions = enriched_anime_stats["load_date"].unique()
    partitioned_anime_stats = [
        enriched_anime_stats.loc[enriched_anime_stats.load_date == part]
        for part in partitions
    ]
    # Join each of the partitions to respective dim_anime partition
    merged_partitions = []
    for part, day in zip(partitioned_anime_stats, partitions):
        keys = surrogate_keys.loc[
            (surrogate_keys["current_as_of"] <= day) & (surrogate_keys["is_current"])
        ]
        merge = part.merge(keys, how="inner", on="anime_id")
        merged_partitions.append(merge)
    # Concatenate all partitions and return DataFrame
    result = pd.concat(merged_partitions)
    # Drop extra columns from dim_anime merge
    result.drop(["current_as_of", "is_current"], axis=1, inplace=True)
    return result


def create_fact_anime_scores(
    cleaned_data: dict, dimension: pd.DataFrame
) -> pd.DataFrame:
    """Creates the fact_anime_scores table based on provided data"""
    # Extract data for enrichment
    surrogate_keys = dimension[["anime_id", "anime_key", "current_as_of", "is_current"]]
    anime_scores = cleaned_data["score"]
    # Normalize the dates
    anime_scores = normalize_date_partition(
        anime_scores, {"load_date": ["2022-03-12", "2022-03-11"]}
    )
    anime_scores = normalize_date_partition(
        anime_scores, {"load_date": ["2022-03-21", "2022-03-20"]}
    )
    anime_scores = normalize_date_partition(
        anime_scores, {"load_date": ["2022-05-12", "2022-05-11"]}
    )
    # Partition data by dates, add surrogate keys, and merge partitions
    partitions = anime_scores["load_date"].unique()
    partitioned_anime_scores = [
        anime_scores.loc[anime_scores.load_date == part] for part in partitions
    ]
    # Merge partitions
    merged_partitions = []
    for part, day in zip(partitioned_anime_scores, partitions):
        keys = surrogate_keys.loc[
            (surrogate_keys["current_as_of"] <= day) & (surrogate_keys["is_current"])
        ]
        merge = part.merge(keys, how="inner", on="anime_id")
        merged_partitions.append(merge)
    result = pd.concat(merged_partitions)
    # Drop extra columns from surrogate key merge
    result.drop(["current_as_of", "is_current"], axis=1, inplace=True)
    return result


def create_warehouse(cleaned_data: dict, conf: dict) -> dict:
    """
    Creates dimension and fact tables based on configuration 
    and returns the data in a dictionary for further processing
    """
    datawarehouse = {}
    # Create dim_anime
    dim_anime = create_dim_anime(cleaned_data["all_anime"], conf)
    datawarehouse["dim_anime"] = dim_anime
    # Create fact tables
    datawarehouse["fact_anime_stats"] = create_fact_anime_stats(cleaned_data, dim_anime)
    datawarehouse["fact_anime_scores"] = create_fact_anime_scores(
        cleaned_data, dim_anime
    )
    return datawarehouse
