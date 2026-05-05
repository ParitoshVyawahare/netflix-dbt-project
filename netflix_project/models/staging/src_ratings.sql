

WITH raw_ratings AS (
  SELECT * FROM MOVIELENS.RAW.RAW_RATING
)

SELECT
  userId AS user_id,
  movieId AS movie_id,
  rating,
  TO_TIMESTAMP_LTZ(timestamps) AS rating_timestamp
FROM raw_ratings