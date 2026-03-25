# ============================================================
# bronze.py
# DLT transformation — Bronze layer
# ============================================================

import dlt
from pyspark.sql import functions as F

# ============================================================
# WHY define constants at the top:
# In DLT, you can't pass arguments into @dlt.table functions
# directly. Constants defined at module level are the clean
# way to share config across all table definitions in the file.
# In a real project these would be imported from _resources/.
# ============================================================

SOURCE_TABLE  = "iot_pipeline.sensors.bronze_sensors"

# ============================================================
# @dlt.table — the core DLT decorator.
#
# name:        the Delta table name DLT will create.
#              This becomes iot_pipeline.sensors_dlt.bronze_sensors
#              in Unity Catalog automatically.
#
# comment:     documents what this table is for.
#              Shows up in the Databricks Data Catalog UI —
#              anyone browsing the catalog can read this.
#              Always write comments — it's professional practice.
#
# table_properties:
#              Key-value metadata stored with the table.
#              "quality" = "bronze" is a convention that lets
#              dashboards and queries filter by layer.
#              "pipeline.name" tags this table as DLT-managed.
# ============================================================

@dlt.table(
    name="bronze_sensors",
    comment="Raw IoT sensor readings ingested from source. No transformations applied.",
    table_properties={
        "quality"       : "bronze",
        "pipeline.name" : "iot-sensor-pipeline",
    }
)
def bronze_sensors():
    """
    Bronze layer — raw ingestion.

    Reads from the source table and adds two audit columns:
    - _ingested_at : when DLT processed this record
    - _source      : where the record came from

    """

    return (
        spark.read.table(SOURCE_TABLE)

        # Add pipeline audit columns
        # _ingested_at: when the DLT pipeline processed this batch
        .withColumn("_ingested_at", F.current_timestamp())

        # _source: tracks where data came from — critical when
        # you have multiple source systems feeding one Bronze table
        .withColumn("_source", F.lit(SOURCE_TABLE))
    )