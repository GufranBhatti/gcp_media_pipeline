-- models/staging/stg_watch_events.sql
SELECT
    CAST(user_id AS INT64) AS user_id,
    CAST(movie_id AS INT64) AS movie_id,
    CAST(watch_duration_mins AS INT64) AS watch_duration_mins,
    CAST(loaded_at AS TIMESTAMP) AS raw_loaded_at
FROM {{ source('streaming_raw', 'watch_events') }}
-- Safety check: Ensure no corrupted negative watch times made it through
WHERE watch_duration_mins > 0