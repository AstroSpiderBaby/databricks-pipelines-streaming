"""
Bronze Streaming Ingestion for Vendor Compliance using Databricks Autoloader.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define storage paths (Unity Catalog Volumes)
input_path = "/Volumes/thebetty/bronze_streaming/landing_zone/vendor_compliance/"
checkpoint_path = "/Volumes/thebetty/bronze_streaming/_checkpoints/vendor_compliance/"
output_path = "/Volumes/thebetty/bronze_streaming/vendor_compliance_clean/"
schema_path = "/Volumes/thebetty/bronze_streaming/_schemas/vendor_compliance/"

# Define schema for incoming CSV
schema = StructType() \
    .add("vendor_id", StringType()) \
    .add("compliance_score", IntegerType()) \
    .add("compliance_status", StringType()) \
    .add("last_audit_date", DateType())

# Read with Autoloader
df_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .schema(schema)
        .load(input_path)
        .withColumn("ingestion_timestamp", current_timestamp())
)

# Write to Delta table with checkpointing
(
    df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
)
