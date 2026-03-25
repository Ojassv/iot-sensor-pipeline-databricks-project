# ============================================================
# gold.py
# DLT transformation — Gold layer
# ============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="gold_hourly_stats",
    comment="Hourly min, max, avg and stddev per machine and sensor type. Consumed by operations dashboard.",
    table_properties={
        "quality"       : "gold",
        "pipeline.name" : "iot-sensor-pipeline",
        "consumer"      : "operations-dashboard",
    }
)
@dlt.expect("reading_count is positive", "reading_count > 0")
@dlt.expect("avg_value is not null",     "avg_value IS NOT NULL")
def gold_hourly_stats():
    """
    Gold layer — hourly aggregations.
    """

    return spark.sql("""
        SELECT
            machine_id,
            sensor_type,
            unit,
            date_trunc('hour', timestamp)    AS hour_window,
            COUNT(*)                          AS reading_count,
            ROUND(AVG(value),    4)           AS avg_value,
            ROUND(MIN(value),    4)           AS min_value,
            ROUND(MAX(value),    4)           AS max_value,
            ROUND(STDDEV(value), 4)           AS stddev_value,
            CURRENT_TIMESTAMP()               AS _gold_processed_at
        FROM LIVE.silver_sensors
        GROUP BY
            machine_id,
            sensor_type,
            unit,
            date_trunc('hour', timestamp)
        ORDER BY
            machine_id,
            sensor_type,
            hour_window
    """)


# ============================================================
# Gold Table 2 — Daily machine health
# ============================================================

@dlt.table(
    name="gold_daily_health",
    comment="Daily health score per machine. Consumed by maintenance team.",
    table_properties={
        "quality"       : "gold",
        "pipeline.name" : "iot-sensor-pipeline",
        "consumer"      : "maintenance-dashboard",
    }
)
@dlt.expect("health_score is valid", "health_score_pct BETWEEN 0 AND 100")
@dlt.expect("machine_id is not null","machine_id IS NOT NULL")
def gold_daily_health():
    """
    Gold layer — daily machine health scores.

    Conditional aggregation pattern:
    AVG(CASE WHEN condition THEN 1.0 ELSE 0.0 END) * 100
    gives percentage of rows meeting the condition —
    same pattern as Notebook 3, now in DLT form.
    """

    return spark.sql("""
        SELECT
            machine_id,
            TO_DATE(timestamp)                                          AS date,
            COUNT(*)                                                     AS total_readings,
            COUNT(DISTINCT sensor_type)                                  AS active_sensors,
            ROUND(AVG(CASE
                WHEN sensor_type = 'temperature' AND value BETWEEN 60.0  AND 120.0 THEN 1.0
                WHEN sensor_type = 'pressure'    AND value BETWEEN 1.0   AND 10.0  THEN 1.0
                WHEN sensor_type = 'vibration'   AND value BETWEEN 0.01  AND 2.0   THEN 1.0
                ELSE 0.0
            END) * 100, 1)                                               AS health_score_pct,
            ROUND(AVG(CASE WHEN sensor_type = 'temperature' THEN value END), 2) AS avg_temperature,
            ROUND(AVG(CASE WHEN sensor_type = 'pressure'    THEN value END), 2) AS avg_pressure,
            ROUND(AVG(CASE WHEN sensor_type = 'vibration'   THEN value END), 2) AS avg_vibration,
            CASE
                WHEN AVG(CASE
                    WHEN sensor_type = 'temperature' AND value BETWEEN 60.0 AND 120.0 THEN 1.0
                    WHEN sensor_type = 'pressure'    AND value BETWEEN 1.0  AND 10.0  THEN 1.0
                    WHEN sensor_type = 'vibration'   AND value BETWEEN 0.01 AND 2.0   THEN 1.0
                    ELSE 0.0
                END) * 100 >= 95 THEN 'NORMAL'
                WHEN AVG(CASE
                    WHEN sensor_type = 'temperature' AND value BETWEEN 60.0 AND 120.0 THEN 1.0
                    WHEN sensor_type = 'pressure'    AND value BETWEEN 1.0  AND 10.0  THEN 1.0
                    WHEN sensor_type = 'vibration'   AND value BETWEEN 0.01 AND 2.0   THEN 1.0
                    ELSE 0.0
                END) * 100 >= 80 THEN 'WARNING'
                ELSE 'CRITICAL'
            END                                                          AS machine_status,
            CURRENT_TIMESTAMP()                                          AS _gold_processed_at
        FROM LIVE.silver_sensors
        GROUP BY machine_id, TO_DATE(timestamp)
        ORDER BY date, machine_id
    """)


# ============================================================
# Gold Table 3 — Anomaly detection
# ============================================================

@dlt.table(
    name="gold_anomalies",
    comment="Sensor readings flagged as anomalous via rolling z-score. Consumed by alert system.",
    table_properties={
        "quality"       : "gold",
        "pipeline.name" : "iot-sensor-pipeline",
        "consumer"      : "alert-system",
    }
)
@dlt.expect("z_score is positive",    "z_score > 0")
@dlt.expect("z_score exceeds threshold", "z_score > 2.0")
def gold_anomalies():
    """
    Gold layer — anomaly detection using rolling window z-score.

    Pattern for window functions in DLT:
    Step 1 — read Silver via dlt.read() into a DataFrame
    Step 2 — apply window functions using PySpark API
    Step 3 — filter to anomalous rows only
    Step 4 — return the result

    We use PySpark window functions here instead of Spark SQL
    because the Window spec needs to be built programmatically.
    Both compile to the same execution plan — this is purely
    a readability choice.
    """

    # Step 1 — read Silver via dlt.read() so DLT tracks dependency
    df_silver = dlt.read("silver_sensors")

    # Step 2 — define rolling window: per machine+sensor, last 50 rows
    window_spec = (
        Window
        .partitionBy("machine_id", "sensor_type")
        .orderBy("timestamp")
        .rowsBetween(-50, 0)
    )

    # Step 3 — compute rolling stats and z-score
    df_with_stats = (
        df_silver
        .withColumn("rolling_avg",
            F.round(F.avg("value").over(window_spec), 4))
        .withColumn("rolling_stddev",
            F.round(F.stddev("value").over(window_spec), 4))
        .withColumn("z_score",
            F.round(
                F.abs(F.col("value") - F.col("rolling_avg")) /
                F.when(F.col("rolling_stddev") > 0, F.col("rolling_stddev"))
                 .otherwise(F.lit(1.0)),
            4))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )

    # Step 4 — return only anomalous rows
    # z_score > 2.0 means more than 2 standard deviations from rolling mean
    return df_with_stats.filter(F.col("z_score") > 2.0)