# Module 2 Homework Solutions

## Question 1: Yellow Taxi December 2020 File Size
**Question:** Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

**Answer:** **128.3 MiB**

**Code to verify:**
```bash
# Download and extract the file
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz | gunzip > yellow_tripdata_2020-12.csv

# Check the file size
ls -lh yellow_tripdata_2020-12.csv
```

**Output:**
```
-rw-r--r--  1 ali  staff   128M Jan 26 12:20 yellow_tripdata_2020-12.csv
```

---

## Question 2: Variable Rendering
**Question:** What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

**Answer:** **green_tripdata_2020-04.csv**

**Explanation:** Looking at the variable definition in the workflow file:
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
```

When:
- `{{inputs.taxi}}` = `green`
- `{{trigger.date | date('yyyy-MM')}}` = `2020-04`

The result is: `green_tripdata_2020-04.csv`

---

## Question 3: Yellow Taxi 2020 Total Rows
**Question:** How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?

**Answer:** **24,648,499**

**Code:**
```python
#!/usr/bin/env python3
import subprocess
import sys

def count_rows_in_csv(url):
    """Download and count rows in a CSV file"""
    try:
        # Download and count rows 
        result = subprocess.run(
            f"wget -qO- {url} | gunzip | wc -l",
            shell=True,
            capture_output=True,
            text=True
        )
        return int(result.stdout.strip()) - 1  # Subtract header row
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return 0

def main():
    taxi_type = sys.argv[1] if len(sys.argv) > 1 else "yellow"
    year = sys.argv[2] if len(sys.argv) > 2 else "2020"
    
    base_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}"
    
    total_rows = 0
    
    # For 2020, we have 12 months
    for month in range(1, 13):
        month_str = f"{month:02d}"
        file_name = f"{taxi_type}_tripdata_{year}-{month_str}.csv.gz"
        url = f"{base_url}/{file_name}"
        
        print(f"Processing {file_name}...")
        rows = count_rows_in_csv(url)
        print(f"  Rows: {rows:,}")
        total_rows += rows
    
    print(f"\nTotal rows for {taxi_type} taxi in {year}: {total_rows:,}")

if __name__ == "__main__":
    main()
```

**Usage:**
```bash
python3 count_rows.py yellow 2020
```

**Breakdown by month:**
```
yellow_tripdata_2020-01.csv.gz:   6,405,008 rows
yellow_tripdata_2020-02.csv.gz:   6,299,354 rows
yellow_tripdata_2020-03.csv.gz:   3,007,292 rows
yellow_tripdata_2020-04.csv.gz:     237,993 rows
yellow_tripdata_2020-05.csv.gz:     348,371 rows
yellow_tripdata_2020-06.csv.gz:     549,760 rows
yellow_tripdata_2020-07.csv.gz:     800,412 rows
yellow_tripdata_2020-08.csv.gz:   1,007,284 rows
yellow_tripdata_2020-09.csv.gz:   1,341,012 rows
yellow_tripdata_2020-10.csv.gz:   1,681,131 rows
yellow_tripdata_2020-11.csv.gz:   1,508,985 rows
yellow_tripdata_2020-12.csv.gz:   1,461,897 rows
─────────────────────────────────────────────
Total:                           24,648,499 rows
```

---

## Question 4: Green Taxi 2020 Total Rows
**Question:** How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?

**Answer:** **1,734,051**

**Code:**
```bash
# Using the same Python script from Question 3
python3 count_rows.py green 2020
```

**Breakdown by month:**
```
green_tripdata_2020-01.csv.gz:     447,770 rows
green_tripdata_2020-02.csv.gz:     398,632 rows
green_tripdata_2020-03.csv.gz:     223,406 rows
green_tripdata_2020-04.csv.gz:      35,612 rows
green_tripdata_2020-05.csv.gz:      57,360 rows
green_tripdata_2020-06.csv.gz:      63,109 rows
green_tripdata_2020-07.csv.gz:      72,257 rows
green_tripdata_2020-08.csv.gz:      81,063 rows
green_tripdata_2020-09.csv.gz:      87,987 rows
green_tripdata_2020-10.csv.gz:      95,120 rows
green_tripdata_2020-11.csv.gz:      88,605 rows
green_tripdata_2020-12.csv.gz:      83,130 rows
─────────────────────────────────────────────
Total:                            1,734,051 rows
```

---

## Question 5: Yellow Taxi March 2021 Rows
**Question:** How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

**Answer:** **1,925,152**

**Code to verify:**
```bash
# Download, extract, and count rows (subtract 1 for header)
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz | gunzip | wc -l
# Output: 1925153 (includes header, so actual rows = 1925153 - 1 = 1,925,152)
```

---

## Question 6: Schedule Trigger Timezone Configuration
**Question:** How would you configure the timezone to New York in a Schedule trigger?

**Answer:** **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**

**Explanation:** Kestra uses the IANA timezone database format. To configure a Schedule trigger for New York timezone:

```yaml
triggers:
  - id: my_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 * * *"
    timezone: America/New_York
```

The correct format is the IANA timezone identifier (`America/New_York`), not:
- `EST` (timezone abbreviation)
- `UTC-5` (offset notation)
- `location` property (wrong property name)

---

