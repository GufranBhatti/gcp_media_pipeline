-- models/staging/stg_movies.sql
SELECT
    CAST(movie_id AS INT64) AS movie_id,
    CAST(title AS STRING) AS title,
    CAST(genre AS STRING) AS genre,
    CAST(rating AS FLOAT64) AS rating
FROM {{ source('streaming_raw', 'movies') }}