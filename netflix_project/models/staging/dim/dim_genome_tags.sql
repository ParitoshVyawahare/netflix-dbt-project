WITH src_tags AS (
    SELECT * FROM {{ ref('src_genome_tag') }}
)

SELECT
    tagid,
    INITCAP(TRIM(tags)) AS tag_name
FROM src_tags