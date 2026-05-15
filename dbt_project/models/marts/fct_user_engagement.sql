-- models/marts/fct_user_engagement.sql

WITH events AS (
    SELECT * FROM {{ ref('stg_watch_events') }}
),

movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
)

SELECT
    e.user_id,
    e.movie_id,
    m.title AS movie_title,
    m.genre AS movie_genre,
    e.watch_duration_mins,
    -- Create a quick KPI flag for the analytics team
    CASE 
        WHEN e.watch_duration_mins >= 90 THEN True 
        ELSE False 
    END AS finished_movie,
    e.raw_loaded_at AS event_timestamp
FROM events e
LEFT JOIN movies m 
    ON e.movie_id = m.movie_id