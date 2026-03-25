# IoT Sensor Data Pipeline

> End-to-end industrial IoT sensor data pipeline built with Apache Spark, Databricks,
> Delta Live Tables and Unity Catalog. Medallion architecture with automated data quality
> validation, anomaly detection and a 4-page AI/BI monitoring dashboard.

---

## Architecture

```
Factory Sensors (simulated)
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  Raw ingestion — 10,000 sensor readings
│  bronze_sensors │  Schema enforcement, audit columns
└────────┬────────┘
         │  @dlt.expect_all_or_drop
         ▼
┌─────────────────┐
│  Silver Layer   │  Cleaned & validated — 9,130 rows
│  silver_sensors │  Zero nulls, duplicates, range violations
└────────┬────────┘
         │
    ┌────┴─────┬──────────────┐
    ▼          ▼              ▼
┌────────┐ ┌────────┐ ┌────────────┐
│ Gold 1 │ │ Gold 2 │ │   Gold 3   │
│ Hourly │ │ Daily  │ │  Anomaly   │
│ Stats  │ │ Health │ │ Detection  │
└────────┘ └────────┘ └────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Databricks AI/BI Dashboard     │
│  4-page monitoring dashboard    │
└─────────────────────────────────┘
```

---

## Tech Stack

| Technology | Usage |
|---|---|
| Apache Spark (PySpark) | Data transformation, aggregations, window functions |
| Databricks | Cloud platform, notebooks, pipeline orchestration |
| Delta Live Tables (DLT) | Production pipeline with built-in data quality |
| Unity Catalog | Data governance and table management |
| Delta Lake | Storage format across all layers |
| Spark SQL | Aggregations, CASE WHEN, date_trunc |
| Python | Data generation, quality framework |
| Databricks AI/BI | 4-page monitoring dashboard |

---

## Repository Structure

```
iot-sensor-pipeline/
│
├── explorations/                      # Development notebooks
│     ├── Notebook 1 — IoT Sensor Data Generator
│     ├── Notebook 2 — Data Exploration & Bronze → Silver
│     ├── Notebook 3 — Silver → Gold Aggregations
│     ├── Notebook 4 — Data Quality Report
│     └── Notebook 5 — Pipeline Summary
│
├── transformations/                   # DLT source files (production)
│     ├── bronze.py                    # Raw ingestion
│     ├── silver.py                    # Cleaning + @dlt.expect_all_or_drop
│     └── gold.py                      # Aggregations + anomaly detection
│
├── Sensor Data Pipeline/              # Complete Databricks project
│     ├── explorations/                # Cloneable into Databricks directly
│     ├── transformations/
│     └── README.md
│
├── Data Insights Dashboards /             # Visual outputs
│     ├── page1_operations_overview.pdf
│     ├── page2_machine_health.pdf
│     ├── page3_anomaly_monitor.pdf
│     ├── page4_pipeline_quality.pdf
│
└── README.md                          # This file
```

**Two ways to use this repo:**
- **Understand the logic** → read `explorations/` notebooks in order
- **Run the pipeline** → clone `Sensor Data Pipeline/` directly into Databricks

---

## Pipeline Layers

### Bronze — Raw Ingestion
Simulates 10,000 factory sensor readings across 5 machines and 3 sensor types
(temperature, pressure, vibration). Intentionally injects ~10% data quality issues
to make the Silver cleaning layer meaningful.

| Column | Description |
|---|---|
| event_id | Unique reading identifier |
| machine_id | MACHINE_01 through MACHINE_05 |
| sensor_type | temperature / pressure / vibration |
| value | Sensor reading (nullable — missing data) |
| timestamp | When the reading was recorded |
| ingested_at | When the pipeline received it |

### Silver — Cleaned & Validated
DLT expectations enforce four quality rules. 648 rows dropped (6.6% of Bronze).

| Rule | Action | Rows dropped |
|---|---|---|
| `value IS NOT NULL` | DROP | 183 |
| `valid temperature range` | DROP | 241 |
| `valid pressure range` | DROP | 240 |
| `valid vibration range` | DROP | 167 |

**Output: 9,130 rows — zero nulls, zero duplicates, zero range violations.**

### Gold — Business-ready Analytics

| Table | Rows | Consumer | Description |
|---|---|---|---|
| `gold_hourly_stats` | 2,500 | Operations dashboard | Hourly avg/min/max/stddev per machine |
| `gold_daily_health` | 35 | Maintenance team | Daily health score per machine (0–100%) |
| `gold_anomalies` | 384 | Alert system | Readings flagged by rolling z-score |

---

## Anomaly Detection

Uses a **rolling z-score** over a 50-reading window per machine and sensor type.

```
z_score = |value - rolling_avg| / rolling_stddev

Threshold: z > 2.0  →  flagged as anomaly
```

**Why z > 2.0?** From the 68-95-99.7 rule — 95% of healthy readings naturally fall
within 2 standard deviations of the mean. A z-score above 2.0 means there is only
a 5% chance the reading occurred naturally, making it statistically worth investigating.

**Why rolling average?** A static all-time average would flag gradual drift as anomalies.
The rolling window adapts to recent machine behaviour so only genuine sudden spikes
are flagged — no false positives from normal operating trends.

| Metric | Value |
|---|---|
| Total anomalies flagged | 384 |
| Anomaly rate | 4.2% of Silver rows |
| Average z-score | 2.33 |
| Highest z-score | 3.19 |

---

## Data Quality Framework

Quality is enforced at two levels:

**1 — DLT native expectations** (`@dlt.expect_all_or_drop` in Silver)
Built into the pipeline — tracked automatically on every run in the DLT event log.

**2 — Quality report table** (`iot_pipeline.sensors.quality_report`)
Notebook 4 runs 15 checks across Bronze, Silver and Gold layers after every pipeline
execution and appends results to an audit Delta table. Enables quality trend tracking
over time.

| Layer | Checks | Result |
|---|---|---|
| Bronze | Row count, null rate, duplicate rate | All passed |
| Silver | Null count, duplicates, range violations, retention | All passed |
| Gold | Machine count, score validity, anomaly rate, day coverage | All passed |
| **Total** | **15 checks** | **15/15 passed (100%)** |

---

## Delta Live Tables Pipeline

The `transformations/` folder contains three DLT source files. The pipeline is
configured to run in **Triggered** mode on **Serverless** compute.

```
Pipeline: Sensor Data Pipeline
Catalog:  iot_pipeline
Schema:   sensors_dlt
Compute:  Serverless
Mode:     Triggered
Runtime:  ~45 seconds end to end
```

DLT resolves the dependency graph automatically from `dlt.read()` calls:

```
bronze_sensors → silver_sensors → gold_hourly_stats
                               → gold_daily_health
                               → gold_anomalies
```

---

## Dashboard

4-page Databricks AI/BI dashboard querying Gold tables directly.

| Page | Source table | Key visuals |
|---|---|---|
| Operations Overview | `gold_hourly_stats` | Line chart — sensor trends over 7 days |
| Machine Health | `gold_daily_health` | Heatmap — health score by machine by day |
| Anomaly Monitor | `gold_anomalies` | Scatter plot — z-scores over time |
| Pipeline Quality | `quality_report` + DLT event log | Quality scorecard, drop rate chart |

---

## How to Run

### Option 1 — Clone Databricks project (recommended)

1. In Databricks workspace → Create → Git folder
2. Repository URL: `https://github.com/Ojassv/iot-sensor-pipeline`
3. Navigate to `Sensor Data Pipeline/`
4. Run `explorations/` notebooks 1 → 5 in order
5. Create DLT pipeline pointing at `transformations/`
6. Set catalog: `iot_pipeline`, schema: `sensors_dlt`
7. Click Start

### Option 2 — Manual setup

1. Copy notebooks from `explorations/` into your Databricks workspace
2. Copy transformation files from `transformations/`
3. Run notebooks 1 → 5 in order
4. Create DLT pipeline with `bronze.py`, `silver.py`, `gold.py` as source files

**Prerequisites:**
- Databricks workspace (Community Edition or higher)
- Unity Catalog enabled
- Serverless compute or a running cluster

---

## Key Concepts Demonstrated

- **Medallion architecture** — Bronze / Silver / Gold layered data organisation
- **Delta Live Tables** — declarative pipeline with `@dlt.table` and `@dlt.expect_all_or_drop`
- **Unity Catalog** — catalog.schema.table governance model
- **Spark window functions** — rolling z-score anomaly detection
- **Partition pruning** — query performance optimisation
- **DLT event log querying** — pipeline observability via `event_log()` function
- **Append vs overwrite** — correct write mode per layer and use case
- **Materialized views vs streaming tables** — batch vs continuous processing
- **Data observability** — longitudinal quality tracking across pipeline runs

---

## Background

This project was built to demonstrate production-grade data engineering skills
for an industrial data engineering role. The scenario mirrors real factory floor
monitoring — collecting sensor readings, validating data quality, detecting
equipment anomalies, and surfacing insights to operations and maintenance teams.

The stack (Spark, Databricks, Delta Lake, Unity Catalog) maps directly to modern
enterprise data engineering. The Medallion architecture mirrors the SAP BW
PSA → DSO → InfoCube pattern used in traditional enterprise data warehouses,
replatformed onto a modern lakehouse architecture.

---

*Built with Apache Spark on Databricks — March 2026*
