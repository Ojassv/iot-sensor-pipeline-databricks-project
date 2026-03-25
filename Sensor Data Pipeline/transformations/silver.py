# ============================================================
# silver.py
# DLT transformation — Silver layer
#
# PURPOSE:
# Read from Bronze, apply data quality expectations,
# clean and validate into a Silver Delta Live Table.
# ============================================================

import dlt
from pyspark.sql import functions as F
from functools import reduce

SENSOR_RANGES = {
    "temperature": {"min": 60.0,  "max": 120.0},
    "pressure":    {"min": 1.0,   "max": 10.0},
    "vibration":   {"min": 0.01,  "max": 2.0},
}

@dlt.table(
    name="silver_sensors",
    comment="Cleaned and validated sensor readings. Nulls, duplicates and out-of-range values removed.",
    table_properties={
        "quality"       : "silver",
        "pipeline.name" : "iot-sensor-pipeline",
    }
)
@dlt.expect_all_or_drop({
    # Rule 1 — no null sensor values
    # SQL expression as a string — DLT evaluates this per row
    "value is not null"         : "value IS NOT NULL",

    # Rule 2 — valid temperature range
    # CASE WHEN returns True for non-temperature rows (not applicable)
    # and checks the range only for temperature rows
    "valid temperature range"   : "NOT (sensor_type = 'temperature' AND (value < 60.0 OR value > 120.0))",

    # Rule 3 — valid pressure range
    "valid pressure range"      : "NOT (sensor_type = 'pressure' AND (value < 1.0 OR value > 10.0))",

    # Rule 4 — valid vibration range
    "valid vibration range"     : "NOT (sensor_type = 'vibration' AND (value < 0.01 OR value > 2.0))",

    # Rule 5 — event_id must exist (can't deduplicate without it)
    "event_id not null"         : "event_id IS NOT NULL",
})
def silver_sensors():
    """
    Silver layer — cleaned and validated data.
    """

    return (
        dlt.read("bronze_sensors")

        # Deduplicate on event_id — keep first occurrence.
        # DLT expectations handle null/range drops above,
        # but deduplication must still be done explicitly
        # as it is not expressible as a row-level SQL rule.
        .dropDuplicates(["event_id"])

        # Add Silver audit column
        .withColumn("_silver_processed_at", F.current_timestamp())

        # Drop the DLT audit columns from Bronze —
        # they were for Bronze lineage only, not needed in Silver
        .drop("_ingested_at", "_source")
    )