"""Kafka -> Delta Bronze -> validated/deduplicated Silver -> windowed Gold."""
from __future__ import annotations

import argparse
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("amount", DecimalType(18, 2)),
        StructField("event_ts", TimestampType()),
    ]
)


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def paths() -> tuple[str, str]:
    root = env("DATA_DIR", "/data")
    return root, env("CHECKPOINT_DIR", "/checkpoints")


def table_path(layer: str, table: str) -> str:
    root, _ = paths()
    return f"{root}/{layer}/{table}"


def checkpoint(name: str) -> str:
    _, root = paths()
    return f"{root}/{name}"


def spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("kafka-spark-delta-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    return spark


def kafka_source(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", env("KAFKA_TOPIC", "events"))
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", env("FAIL_ON_DATA_LOSS", "true"))
        .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "50000"))
        .load()
    )


def bronze(spark: SparkSession) -> None:
    raw = kafka_source(spark)
    bronze_df = raw.select(
        F.col("value").cast("string").alias("event_json"),
        F.col("key").cast("string").alias("message_key"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),
    )
    (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint("bronze"))
        .partitionBy("ingestion_date")
        .trigger(processingTime=env("TRIGGER_INTERVAL", "5 seconds"))
        .start(table_path("bronze", "events"))
        .awaitTermination()
    )


def parsed_bronze(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("delta").load(table_path("bronze", "events"))
        .withColumn("event", F.from_json("event_json", EVENT_SCHEMA))
        .select("*", "event.*")
        .drop("event")
        .withColumn("event_ts", F.to_utc_timestamp("event_ts", "UTC"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
    )


def quality_rules(df: DataFrame) -> DataFrame:
    valid = (
        F.col("event_id").isNotNull()
        & (F.length(F.trim("event_id")) > 0)
        & F.col("event_type").isin("view", "cart", "purchase", "refund")
        & F.col("user_id").isNotNull()
        & (F.length(F.trim("user_id")) > 0)
        & F.col("event_ts").isNotNull()
        & (F.col("quantity").isNull() | (F.col("quantity") > 0))
        & (F.col("amount").isNull() | (F.col("amount") >= 0))
    )
    return df.withColumn("is_valid", F.coalesce(valid, F.lit(False))).withColumn(
        "quality_error",
        F.when(
            F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0),
            "missing_event_id",
        )
        .when(F.col("event_type").isNull(), "missing_event_type")
        .when(~F.col("event_type").isin("view", "cart", "purchase", "refund"), "invalid_event_type")
        .when(F.col("user_id").isNull() | (F.length(F.trim("user_id")) == 0), "missing_user_id")
        .when(F.col("event_ts").isNull(), "invalid_event_timestamp")
        .when(F.col("quantity").isNotNull() & (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("amount").isNotNull() & (F.col("amount") < 0), "invalid_amount")
        .otherwise(None),
    )


def silver(spark: SparkSession) -> None:
    marked = quality_rules(parsed_bronze(spark))
    valid = (
        marked.filter("is_valid")
        .withWatermark("event_ts", env("DEDUP_WATERMARK", "2 hours"))
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
    )
    valid_query = (
        valid.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", checkpoint("silver"))
        .trigger(processingTime=env("TRIGGER_INTERVAL", "5 seconds"))
        .start(table_path("silver", "events"))
    )
    invalid_query = (
        marked.filter("NOT is_valid")
        .withColumn("quarantined_at", F.current_timestamp())
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint("quarantine"))
        .trigger(processingTime=env("TRIGGER_INTERVAL", "5 seconds"))
        .start(table_path("silver", "quarantine"))
    )
    spark.streams.awaitAnyTermination()


def gold(spark: SparkSession) -> None:
    events = spark.readStream.format("delta").load(table_path("silver", "events"))
    purchases = events.filter(F.col("event_type") == "purchase")
    aggregates = (
        purchases.withWatermark("event_ts", env("GOLD_WATERMARK", "2 hours"))
        .groupBy(F.window("event_ts", env("GOLD_WINDOW", "5 minutes")), "product_id")
        .agg(
            F.count("event_id").alias("purchase_count"),
            F.sum(F.coalesce("amount", F.lit(0))).alias("gross_revenue"),
            F.sum(F.coalesce("quantity", F.lit(0))).alias("units_sold"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "purchase_count",
            "gross_revenue",
            "units_sold",
            F.current_timestamp().alias("updated_at"),
        )
    )
    (
        aggregates.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint("gold"))
        .partitionBy("window_start")
        .trigger(processingTime=env("TRIGGER_INTERVAL", "5 seconds"))
        .start(table_path("gold", "product_sales_5m"))
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("bronze", "silver", "gold"), required=True)
    args = parser.parse_args()
    spark = spark_session()
    try:
        {"bronze": bronze, "silver": silver, "gold": gold}[args.mode](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
