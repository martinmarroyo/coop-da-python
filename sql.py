from datetime import datetime

today = datetime.now().strftime("%Y-%m-%d")

# Create Schema
CREATE_SCHEMA = """
    DROP SCHEMA IF EXISTS anime CASCADE;
    CREATE SCHEMA IF NOT EXISTS anime;
"""
# Dim Day
CREATE_DIM_DAY = """
    CREATE TABLE IF NOT EXISTS public.dim_day (
        day_key SERIAL,
        _date DATE,
        day_of_month INT,
        month_num INT,
        quarter INT,
        _year INT,
        month_name TEXT,
        day_of_week TEXT,
        day_of_year INT,
        week_of_year INT,
        day_of_quarter INT,
        month_start_day TEXT,
        month_end_day TEXT,
        last_day_of_month INT,
        total_days_in_year INT,
        PRIMARY KEY (day_key)
    );

    TRUNCATE TABLE public.dim_day RESTART IDENTITY;

    -- Insert NULL row
    INSERT INTO public.dim_day(
        _date, day_of_month, month_num, quarter, 
        _year, month_name, day_of_week, day_of_year, 
        week_of_year, day_of_quarter, 
        month_start_day, month_end_day, last_day_of_month, 
        total_days_in_year
    )
    VALUES ('1000-12-31', NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL
    );

    -- Insert Dim_Day
    INSERT INTO public.dim_day(
        _date, day_of_month, month_num, quarter, _year, 
        month_name, day_of_week, day_of_year, week_of_year, 
        day_of_quarter, month_start_day, 
        month_end_day, last_day_of_month, 
        total_days_in_year)
    WITH dim_day AS (
    SELECT
        ROW_NUMBER() OVER
        (ORDER BY
            series.day)
        AS date_key
        ,series.day::DATE
        AS _date
        ,EXTRACT(DAY FROM series.day::DATE)
        AS day_of_month
        ,EXTRACT(MONTH FROM series.day::DATE)
        AS month_num
        ,EXTRACT(QUARTER FROM series.day::DATE)
        AS quarter
        ,EXTRACT(YEAR FROM series.day::DATE)
        AS _year
        ,TO_CHAR(series.day::DATE,'Month')
        AS month_name
        ,TO_CHAR(series.day::DATE,'Day')
        AS day_of_week
        ,EXTRACT(DOY FROM series.day::DATE)
        AS day_of_year
        ,EXTRACT(WEEK FROM series.day::DATE)
        AS week_of_year
    FROM
        GENERATE_SERIES
            ('1970-01-01'::DATE
            ,'2050-01-01'::DATE,'1 Day')
        AS series(day)
    )
    SELECT
        _date
        ,day_of_month
        ,month_num
        ,quarter
        ,_year
        ,month_name
        ,day_of_week
        ,day_of_year
        ,week_of_year
        ,ROW_NUMBER() OVER
        (PARTITION BY
            quarter
            ,_year
        ORDER BY
            _date
        )
        AS day_of_quarter
        ,TO_CHAR((MAKE_DATE(_year::INT,month_num::INT,1))::DATE
                ,'Day')
        AS month_start_day
        ,TO_CHAR((MAKE_DATE(_year::INT,month_num::INT,1)
                + '1 Month'::INTERVAL 
                - '1 Day'::INTERVAL)::DATE
                ,'Day')
        AS month_end_day
        ,EXTRACT(DAY FROM 
                (MAKE_DATE(_year::INT,month_num::INT,1)
                + '1 Month'::INTERVAL 
                - '1 Day'::INTERVAL)::DATE)
        AS last_day_of_month
        ,EXTRACT(DOY FROM MAKE_DATE(_year::INT,12,31))
        AS total_days_in_year
    FROM
        dim_day
    ORDER BY
        date_key;
        
    CREATE INDEX IF NOT EXISTS idx_date ON public.dim_day(_date);
"""

# Metadata
ADD_METADATA = """
        -- Anime Stats and Scores Raw
    COMMENT ON TABLE anime.anime_stats_and_scores_raw
        IS '''A listing of all animes with their respective statistics and scores from MyAnimeList. 
            Columns 1-10 denote the number of raw votes for each respective score''';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.anime_id
        IS 'The My Anime List id of the anime';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.load_date
        IS 'Date/time that scores were extracted and loaded';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.anime_title
        IS 'The title of the anime';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.status
        IS 'Airing status ("Finished Airing" "Currently Airing" "Not yet aired")';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.rating
        IS 'Anime audience rating ("G - All Ages" "PG - Children" "PG-13 - Teens 13 or older" "R - 17+ (violence & profanity)" "R+ - Mild Nudity" "Rx - Hentai")';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.score
        IS 'Score';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.favorites
        IS 'Number of users who have favorited this entry';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.airing
        IS 'Indicates whether an anime is currently airing';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.aired_from
        IS 'Date that anime first started airing';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.aired_to
        IS 'Date that anime stopped airing if off the air';
        
    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.watching
        IS 'Number of users watching the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.completed
        IS 'Number of users who have completed the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.on_hold
        IS 'Number of users who have put the resource on hold';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.dropped
        IS 'Number of users who have dropped the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.plan_to_watch
        IS 'Number of users who have planned to watch the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_raw.total
        IS 'Total number of users who have the resource added to their lists'; 
    -- Anime Stats and Scores Percentage
    COMMENT ON TABLE anime.anime_stats_and_scores_pct
        IS '''A listing of all animes with their respective statistics and scores from MyAnimeList. 
            Columns 1-10 denote the percentage of votes given for each respective score for that anime.''';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.anime_id
        IS 'The My Anime List id of the anime';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.load_date
        IS 'Date/time that scores were extracted and loaded';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.anime_title
        IS 'The title of the anime';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.status
        IS 'Airing status ("Finished Airing" "Currently Airing" "Not yet aired")';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.rating
        IS 'Anime audience rating ("G - All Ages" "PG - Children" "PG-13 - Teens 13 or older" "R - 17+ (violence & profanity)" "R+ - Mild Nudity" "Rx - Hentai")';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.score
        IS 'Score';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.favorites
        IS 'Number of users who have favorited this entry';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.airing
        IS 'Indicates whether an anime is currently airing';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.aired_from
        IS 'Date that anime first started airing';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.aired_to
        IS 'Date that anime stopped airing if off the air';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.watching
        IS 'Number of users watching the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.completed
        IS 'Number of users who have completed the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.on_hold
        IS 'Number of users who have put the resource on hold';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.dropped
        IS 'Number of users who have dropped the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.plan_to_watch
        IS 'Number of users who have planned to watch the resource';

    COMMENT ON COLUMN anime.anime_stats_and_scores_pct.total
        IS 'Total number of users who have the resource added to their lists'; 
    -- All Anime 
    COMMENT ON TABLE anime.all_anime
        IS 'All suitable for work anime titles from My Anime List';

    COMMENT ON COLUMN anime.all_anime.anime_id
        IS 'The My Anime List id of the anime';

    COMMENT ON COLUMN anime.all_anime.anime_title
        IS 'The title of the anime';

    COMMENT ON COLUMN anime.all_anime.status
        IS 'Airing status ("Finished Airing" "Currently Airing" "Not yet aired")';

    COMMENT ON COLUMN anime.all_anime.rating
        IS 'Anime audience rating ("G - All Ages" "PG - Children" "PG-13 - Teens 13 or older" "R - 17+ (violence & profanity)" "R+ - Mild Nudity" "Rx - Hentai")';

    COMMENT ON COLUMN anime.all_anime.score
        IS 'Score';

    COMMENT ON COLUMN anime.all_anime.favorites
        IS 'Number of users who have favorited this entry';

    COMMENT ON COLUMN anime.all_anime.airing
        IS 'Indicates whether an anime is currently airing';

    COMMENT ON COLUMN anime.all_anime.aired_from
        IS 'Date that anime first started airing';

    COMMENT ON COLUMN anime.all_anime.aired_to
        IS 'Date that anime stopped airing if off the air';
    -- Anime Scores
    COMMENT ON COLUMN anime.scores.anime_id
        IS 'The id of the anime';

    COMMENT ON COLUMN anime.scores.score
        IS 'Scoring value';

    COMMENT ON COLUMN anime.scores.votes
        IS 'Number of votes for this score';

    COMMENT ON COLUMN anime.scores.percentage
        IS 'Percentage of votes for this score';

    COMMENT ON COLUMN anime.scores.load_date
        IS 'Date/time that scores were extracted and loaded';

    -- Anime Stats
    COMMENT ON COLUMN anime.stats.anime_id
        IS 'The id of the anime';

    COMMENT ON COLUMN anime.stats.watching
        IS 'Number of users watching the resource';

    COMMENT ON COLUMN anime.stats.completed
        IS 'Number of users who have completed the resource';

    COMMENT ON COLUMN anime.stats.on_hold
        IS 'Number of users who have put the resource on hold';

    COMMENT ON COLUMN anime.stats.dropped
        IS 'Number of users who have dropped the resource';

    COMMENT ON COLUMN anime.stats.plan_to_watch
        IS 'Number of users who have planned to watch the resource';

    COMMENT ON COLUMN anime.stats.total
        IS 'Total number of users who have the resource added to their lists';

    COMMENT ON COLUMN anime.stats.load_date
        IS 'Date/time that stats were extracted and loaded';    
"""


# Analyzing Columns Statistics
ANALYZE_COLUMN_STATS = """
    ANALYZE public.dim_day;
    ANALYZE anime.all_anime;
    ANALYZE anime.stats;
    ANALYZE anime.scores;
    ANALYZE anime.anime_stats_and_scores_raw;
    ANALYZE anime.anime_stats_and_scores_pct;
"""