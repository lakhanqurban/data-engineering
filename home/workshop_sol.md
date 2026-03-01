# dlt Homework Solutions

## Pipeline Setup

The pipeline script ([taxi_pipeline.py](taxi_pipeline.py)) loads NYC Yellow Taxi trip data from the custom API into DuckDB using `dlt`'s REST API source.

```python
import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "/",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }
)

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
)

load_info = pipeline.run(source)
print(load_info)
```

The pipeline loaded **10,000 rows** into the `taxi_data.rides` table.

---

## Question 1: What is the start date and end date of the dataset?

**Query:**

```sql
SELECT
    MIN(trip_pickup_date_time) AS start_date,
    MAX(trip_pickup_date_time) AS end_date
FROM taxi_data.rides;
```

**Answer: 2009-06-01 to 2009-07-01** 

---

## Question 2: What proportion of trips are paid with credit card?

**Query:**

```sql
SELECT
    payment_type,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM taxi_data.rides
GROUP BY payment_type
ORDER BY cnt DESC;
```

**Answer: 26.66%** 

---

## Question 3: What is the total amount of money generated in tips?

**Query:**

```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM taxi_data.rides;
```

**Answer: $6,063.41** 
