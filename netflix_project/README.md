# 🎬 Netflix Data Analytics | End-to-End ELT Pipeline with dbt, Snowflake & AWS

A production-grade **ELT (Extract, Load, Transform)** data pipeline built using **dbt (Data Build Tool)**, **Snowflake**, and **Amazon S3**, with data visualization powered by **Looker Studio**. This project demonstrates modern data engineering best practices including modular SQL modeling, incremental loads, snapshot-based slowly changing dimensions, automated testing, and comprehensive documentation.

---

## 📌 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Data Pipeline Layers](#data-pipeline-layers)
- [dbt Features Used](#dbt-features-used)
- [Snowflake Configuration](#snowflake-configuration)
- [Data Visualization — Looker Studio](#data-visualization--looker-studio)
- [Getting Started](#getting-started)
- [Key Learnings](#key-learnings)

---

## Architecture Overview
<img width="2020" height="880" alt="image" src="https://github.com/user-attachments/assets/def9f063-fda2-450b-95ab-2ea1e1444ee5" />
The pipeline follows the **ELT pattern**:

1. **Extract** — Raw CSV files sourced from the MovieLens 20M dataset
2. **Load** — Data loaded into Snowflake's RAW schema via Amazon S3 external stages using `COPY INTO`
3. **Transform** — dbt handles all transformations across staging, dimension, and fact layers inside Snowflake
4. **Visualize** — Looker Studio connects to the serving layer for dashboards and analysis

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **dbt Core** | Data transformation, testing, documentation, snapshots |
| **Snowflake** | Cloud data warehouse (compute & storage) |
| **Amazon S3** | External stage for raw data file storage |
| **Looker Studio** | Data visualization and dashboard creation |
| **GitHub** | Version control and collaboration |
| **Python (venv)** | dbt runtime environment |

---

## Dataset

**MovieLens 20M Dataset** by GroupLens Research

🔗 [https://grouplens.org/datasets/movielens/20m/](https://grouplens.org/datasets/movielens/20m/)

The dataset contains **20 million ratings** and **465,000 tag applications** applied to **27,000 movies** by **138,000 users**. It includes:

| File | Description | Rows |
|------|-------------|------|
| `movies.csv` | Movie titles and genres | 27,278 |
| `ratings.csv` | User movie ratings (0.5–5.0 scale) | 20,000,263 |
| `tags.csv` | User-applied tags to movies | 465,564 |
| `links.csv` | Mappings to IMDb and TMDb IDs | 27,278 |
| `genome-tags.csv` | Tag genome tag names | 1,128 |
| `genome-scores.csv` | Tag genome relevance scores | 11,709,768 |

---

## Project Structure

---

## Data Pipeline Layers

### 1️⃣ Raw Layer (Snowflake `RAW` Schema)

Raw data is loaded from **Amazon S3** into Snowflake using external stages and `COPY INTO` commands:

```sql
CREATE STAGE netflixproject
  URL = 's3://netflixproject-paritosh'
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');

COPY INTO raw_movies
FROM '@netflixproject/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

**Raw tables created:** `raw_movies`, `raw_rating`, `raw_tag`, `raw_link`, `raw_genome_tags`, `raw_genome_score`

### 2️⃣ Staging Layer (dbt Views)

Staging models clean and standardize the raw data — renaming columns, casting data types, and applying consistent conventions:

```sql
-- src_ratings.sql
WITH raw_ratings AS (
    SELECT * FROM MOVIELENS.RAW.RAW_RATING
)
SELECT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    TO_TIMESTAMP_LTZ(timestamps) AS rating_timestamp
FROM raw_ratings
```

**Materializations:** Views (lightweight, always up-to-date) except `src_tags` which is a table for snapshot compatibility.

### 3️⃣ Dimension Tables (dbt Tables)

Dimension models create business-friendly lookup tables:

- **`dim_movies`** — Unique movies with titles and genres
- **`dim_users`** — Distinct users derived from a UNION of ratings and tags
- **`dim_genome_tags`** — Genome tag ID-to-name mapping
- **`dim_movies_from_tags`** — Movies enriched with genome tags and relevance scores via multi-table JOINs

### 4️⃣ Fact Tables (dbt Tables / Incremental)

Fact models capture measurable business events:

- **`fact_ratings`** — User-movie ratings with timestamps (incremental materialization)
- **`fact_genome_score`** — Tag relevance scores per movie

---

## dbt Features Used

### Materializations

| Strategy | Where Used | Why |
|----------|-----------|-----|
| **View** | Staging models (`src_*`) | Lightweight; always reflects current raw data |
| **Table** | Dimension & fact models | Pre-computed for query performance |
| **Incremental** | `fact_ratings` | Efficiently appends new data without full table rebuild on 20M+ rows |

```sql
-- fact_ratings.sql (Incremental)
{{ config(materialized='incremental', unique_key='rating_id') }}

SELECT ...
FROM {{ ref('src_ratings') }}
{% if is_incremental() %}
  WHERE rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}
```

### Sources & Refs

- **`source()`** — References raw Snowflake tables defined in `sources.yml`, enabling lineage tracking
- **`ref()`** — References other dbt models, building a DAG (Directed Acyclic Graph) of dependencies

```yaml
# sources.yml
sources:
  - name: netflix
    database: MOVIELENS
    schema: RAW
    tables:
      - name: raw_movies
      - name: raw_rating
      - name: raw_tag
      - name: raw_link
      - name: raw_genome_tags
      - name: raw_genome_score
```

### Testing

dbt tests validate data quality at the model and column level:

```yaml
# schema.yml
models:
  - name: dim_users
    columns:
      - name: user_id
        description: Unique user identifier
        tests:
          - unique
          - not_null
```

**Test types used:**
- **`unique`** — Ensures no duplicate values in key columns
- **`not_null`** — Ensures critical columns have no NULL values
- **Schema tests** — Applied declaratively via YAML
- **Singular tests** — Custom SQL tests in the `/tests` directory

Run tests with: `dbt test`

### Snapshots (SCD Type 2)

Snapshots track **slowly changing dimensions** — capturing how data changes over time:

```sql
{% snapshot snap_tags %}
{{
    config(
        target_schema='snapshots',
        unique_key=['user_id','movie_id','movie_tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id','movie_id','movie_tag']) }} AS row_key,
    user_id,
    movie_id,
    movie_tag,
    CAST(tag_timestamp AS TIMESTAMP_NTZ) AS tag_timestamp
FROM {{ ref('src_tags') }}
LIMIT 100
{% endsnapshot %}
```

This creates a versioned history table with `dbt_valid_from` and `dbt_valid_to` columns — enabling point-in-time analysis.

Run snapshots with: `dbt snapshot`

### Packages — dbt_utils

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

**Key macro used:** `generate_surrogate_key()` — creates deterministic hash keys from multiple columns for snapshot unique keys.

Install packages with: `dbt deps`

### Documentation

dbt auto-generates a documentation site with model descriptions, column-level docs, and a visual DAG:

```bash
dbt docs generate
dbt docs serve
```

### Project Configuration

```yaml
# dbt_project.yml
models:
  netflix_project:
    +materialized: view

    staging:
      dim:
        +materialized: table
    fact:
      +materialized: table
```

---

## Snowflake Configuration

### Database & Schema Architecture

| Schema | Purpose | Objects |
|--------|---------|---------|
| `MOVIELENS.RAW` | Raw data landing zone | 6 tables loaded from S3 |
| `MOVIELENS.DEVS` | dbt development environment | Views (staging) + Tables (dim/fact) |
| `MOVIELENS.SNAPSHOTS` | Snapshot history tables | SCD Type 2 versioned tables |

### Connection Profile

```yaml
# profiles.yml
netflix_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: <username>
      password: <password>
      role: ACCOUNTADMIN
      database: MOVIELENS
      warehouse: COMPUTE_WH
      schema: DEVS
      threads: 1
```

### S3 Integration

Data flows from **Amazon S3 → Snowflake** using:
- **External Stage** — `netflixproject` stage pointing to the S3 bucket
- **`COPY INTO`** — Bulk loading with CSV file format options
- **`ON_ERROR = CONTINUE`** — Graceful handling of malformed rows

---

## Data Visualization — Looker Studio

The final serving layer (DEVS schema) connects to **Looker Studio** for interactive dashboards covering:

- 🎥 **Movie Analysis** — Genre distribution, top-rated films, rating trends over time
- 👥 **User Behavior** — Rating patterns, most active users, tag activity
- 🧬 **Genome Insights** — Tag relevance across movies, genre-tag correlations
- 📊 **KPI Metrics** — Total ratings, average scores, user engagement

---

## Getting Started

### Prerequisites

- Python 3.8+
- Snowflake account
- AWS account (S3 bucket with MovieLens data)

### Installation

```bash
git clone https://github.com/ParitoshVyawahare/netflix-dbt-project.git
cd netflix-dbt-project

python3 -m venv .venv
source .venv/bin/activate

pip install dbt-snowflake

dbt deps
```

### Running the Pipeline

```bash
dbt run          # Run all models
dbt test         # Run all tests
dbt snapshot     # Execute snapshots
dbt docs generate && dbt docs serve  # Generate & view docs
```

### Useful dbt Commands

| Command | Description |
|---------|-------------|
| `dbt run` | Execute all models |
| `dbt run --select model_name` | Run a specific model |
| `dbt run --select +model_name` | Run model + upstream dependencies |
| `dbt test` | Run all tests |
| `dbt snapshot` | Execute snapshot models |
| `dbt deps` | Install packages from `packages.yml` |
| `dbt docs generate` | Generate project documentation |
| `dbt docs serve` | Launch documentation web server |
| `dbt clean` | Remove compiled files in `/target` |
| `dbt debug` | Test database connection |

---

## Key Learnings

- **Modular SQL with dbt** — Breaking transformations into reusable, testable models with clear lineage
- **ELT over ETL** — Loading raw data first, then transforming inside the warehouse using dbt
- **Incremental Models** — Efficiently processing 20M+ rows without full refreshes
- **SCD Type 2 Snapshots** — Tracking historical changes in user-generated tags
- **Data Quality with Tests** — Catching issues early with automated `unique` and `not_null` tests
- **Surrogate Keys** — Using `dbt_utils.generate_surrogate_key()` for deterministic hashing
- **Source Freshness** — Monitoring raw data staleness with dbt source definitions
- **Cloud-Native Stack** — Leveraging S3 for storage, Snowflake for compute, and Looker for visualization

---

## 📄 License

This project uses the [MovieLens 20M Dataset](https://grouplens.org/datasets/movielens/20m/) provided by GroupLens Research under their terms of use.

---

**Built with ❤️ by [Paritosh Vyawahare](https://github.com/ParitoshVyawahare)**



