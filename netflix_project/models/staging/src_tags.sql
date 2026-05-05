

WITH raw_tags AS (
    SELECT * FROM MOVIELENS.RAW.RAW_TAG
)

SELECT
    userId AS user_id,
    movieId AS movie_id,
    movie_tag,
    TO_TIMESTAMP_LTZ(timestamps) AS tag_timestamp
FROM raw_tags