# ============================================================
# bronze.py
# DLT transformation — Bronze layer
# ============================================================

import dlt
from pyspark.sql import functions as F

SOURCE_TABLE  = "iot_pipeline.sensors.bronze_sensors"

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