-- tests/assert_positive_watch_duration.sql
-- This test will fail if it finds ANY records where watch duration is 0 or negative.

SELECT
    user_id,
    movie_id,
    watch_duration_mins
FROM {{ ref('fct_user_engagement') }}
WHERE watch_duration_mins <= 0