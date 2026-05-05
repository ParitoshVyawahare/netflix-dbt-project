with src_movies AS (

select * from {{ref("src_movies")}}

)
select 
    movie_id,
    INITCAP(TRIM(title)) as movie_title,
    SPLIT(genres, '|') as genres_new,
    genres
from src_movies