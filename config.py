"""
Configuration that drives transformations for the given data table
"""

RAW_DATA_LOC = [
    r'/src/python-env/data/all_anime.csv', 
    r'/src/python-env/data/anime_stats.csv', 
    r'/src/python-env/data/anime_scores.csv'
]
ALL_ANIME = {
    "rename": {"id": "anime_id", "title": "anime_title",},
    "datecols": ["load_date", "aired_from", "aired_to"],
    "dupe_index": ["anime_id"]
}

ANIME_STATS = {
    "datecols": ["load_date"],
    "dupe_index": ["anime_id"]
}

ANIME_SCORES = {
    "datecols": ["load_date"],
    "dupe_index": ["anime_id", "score"]
}

ANIME_VOTES_RAW = {
    "index": "anime_id",
    "columns": "score",
    "values": "votes"
}
