# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time events on Databricks: Kafka → Bronze → Silver → Gold
# MAGIC
# MAGIC Run every cell from top to bottom on **Serverless** compute. The default `demo` mode
# MAGIC seeds a few readable sample events, then processes Bronze → Silver → Gold incrementally.
# MAGIC Switch `SOURCE_MODE` to `kafka` only after you have a broker reachable from this workspace.
# MAGIC This Free Edition notebook uses `AvailableNow` (bounded incremental runs); rerun it to
# MAGIC process newly arrived Kafka messages. It is not an always-on low-latency job.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType, TimestampType
)

# Start with the sample so you can see the full output without configuring Kafka.
SOURCE_MODE = "demo"  # change to "kafka" when the connection values below are ready
KAFKA_BOOTSTRAP_SERVERS = "<your-broker-host>:9092"
KAFKA_TOPIC = "events"

# Uses your current catalog and a dedicated schema for this project.
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "realtime_pipeline"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`pipeline_state`")

def table(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/pipeline_state"
BRONZE = table("bronze_events")
SILVER = table("silver_events")
QUARANTINE = table("silver_quarantine")
GOLD = table("gold_product_sales_5m")

print(f"Mode: {SOURCE_MODE}")
print(f"Tables: {CATALOG}.{SCHEMA}")
print(f"Checkpoints: {CHECKPOINT_ROOT}")

# COMMAND ----------

# In demo mode, put a few sample Kafka-shaped records in Bronze once. They include
# two purchases for sku-7, one view, and one bad record that should be quarantined.
if SOURCE_MODE == "demo" and not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.bronze_events"):
    demo_rows = [
        ('evt-1001', '{"event_id":"evt-1001","event_type":"purchase","user_id":"u-42","product_id":"sku-7","quantity":2,"amount":39.98,"event_ts":"2026-11-03T01:00:00Z"}', 'events', 0, 0),
        ('evt-1002', '{"event_id":"evt-1002","event_type":"purchase","user_id":"u-21","product_id":"sku-7","quantity":1,"amount":10.00,"event_ts":"2026-11-03T04:00:00Z"}', 'events', 0, 1),
        ('evt-1003', '{"event_id":"evt-1003","event_type":"view","user_id":"u-42","product_id":"sku-9","quantity":1,"amount":null,"event_ts":"2026-11-03T04:01:00Z"}', 'events', 1, 0),
        ('evt-bad', '{"event_id":"evt-bad","event_type":"purchase","user_id":"  ","product_id":"sku-7","quantity":1,"amount":5.00,"event_ts":"2026-11-03T04:02:00Z"}', 'events', 1, 1),
    ]
    demo_schema = StructType([
        StructField("message_key", StringType(), True),
        StructField("event_json", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
    ])
    (spark.createDataFrame(demo_rows, demo_schema)
        .withColumn("kafka_timestamp", F.current_timestamp())
        .withColumn("ingested_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").saveAsTable(BRONZE))
    print("Seeded four demo events into Bronze.")

# In Kafka mode, ingest raw JSON and Kafka coordinates into Bronze. The checkpoint
# preserves offsets so reruns continue from the last processed message.
if SOURCE_MODE == "kafka":
    kafka_raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", 50000)
        .load())
    bronze_stream = kafka_raw.select(
        F.col("value").cast("string").alias("event_json"),
        F.col("key").cast("string").alias("message_key"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )
    q = (bronze_stream.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .trigger(availableNow=True).toTable(BRONZE))
    q.awaitTermination()
elif SOURCE_MODE != "demo":
    raise ValueError('SOURCE_MODE must be "demo" or "kafka"')

display(spark.table(BRONZE).select("message_key", "event_json", "topic", "partition", "offset"))

# COMMAND ----------

# Parse the JSON event contract and route clean and invalid records separately.
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("amount", DecimalType(18, 2)),
    StructField("event_ts", TimestampType()),
])

parsed = (spark.readStream.table(BRONZE)
    .withColumn("event", F.from_json("event_json", event_schema))
    .select("topic", "partition", "offset", "ingested_at", "event.*")
    .withColumn("event_type", F.lower(F.trim("event_type"))))

valid_condition = (
    F.col("event_id").isNotNull() & (F.length(F.trim("event_id")) > 0)
    & F.col("event_type").isin("view", "cart", "purchase", "refund")
    & F.col("user_id").isNotNull() & (F.length(F.trim("user_id")) > 0)
    & F.col("event_ts").isNotNull()
    & (F.col("quantity").isNull() | (F.col("quantity") > 0))
    & (F.col("amount").isNull() | (F.col("amount") >= 0))
)
marked = (parsed.withColumn("is_valid", F.coalesce(valid_condition, F.lit(False)))
    .withColumn("quality_error",
        F.when(F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "missing_event_id")
        .when(F.col("event_type").isNull(), "missing_event_type")
        .when(~F.col("event_type").isin("view", "cart", "purchase", "refund"), "invalid_event_type")
        .when(F.col("user_id").isNull() | (F.length(F.trim("user_id")) == 0), "missing_user_id")
        .when(F.col("event_ts").isNull(), "invalid_event_timestamp")
        .when(F.col("quantity").isNotNull() & (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("amount").isNotNull() & (F.col("amount") < 0), "invalid_amount")))

silver_valid = (marked.filter("is_valid")
    .withWatermark("event_ts", "2 hours")
    .dropDuplicatesWithinWatermark(["event_id"])
    .withColumn("event_date", F.to_date("event_ts")))
silver_bad = marked.filter("NOT is_valid").withColumn("quarantined_at", F.current_timestamp())

valid_q = (silver_valid.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
    .trigger(availableNow=True).toTable(SILVER))
bad_q = (silver_bad.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/quarantine")
    .trigger(availableNow=True).toTable(QUARANTINE))
valid_q.awaitTermination()
bad_q.awaitTermination()

display(spark.table(SILVER).orderBy("event_ts"))
display(spark.table(QUARANTINE).select("event_id", "user_id", "quality_error"))

# COMMAND ----------

# Gold counts purchases in five-minute event-time windows. The demo has events
# several hours apart so the watermark can close its first window in this run.
purchases = (spark.readStream.table(SILVER)
    .filter(F.col("event_type") == "purchase")
    .withWatermark("event_ts", "30 minutes"))
gold_rows = (purchases.groupBy(F.window("event_ts", "5 minutes"), "product_id")
    .agg(F.count("event_id").alias("purchase_count"),
         F.sum(F.coalesce("amount", F.lit(0))).alias("gross_revenue"),
         F.sum(F.coalesce("quantity", F.lit(0))).alias("units_sold"))
    .select(F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"), "product_id",
            "purchase_count", "gross_revenue", "units_sold"))
gold_q = (gold_rows.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold")
    .trigger(availableNow=True).toTable(GOLD))
gold_q.awaitTermination()

display(spark.table(GOLD).orderBy("window_start", "product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you should see
# MAGIC
# MAGIC - **Bronze:** four original JSON messages and source coordinates.
# MAGIC - **Silver:** three valid events; repeated `event_id` values are deduplicated.
# MAGIC - **Quarantine:** `evt-bad` with `missing_user_id`.
# MAGIC - **Gold:** two purchase windows for `sku-7`; totals are 2 purchases, 49.98 revenue,
# MAGIC   and 3 units across the two rows.
# MAGIC
# MAGIC Each layer has its own checkpoint under the Unity Catalog volume. Keep those files
# MAGIC between runs. To start a fresh demo, remove the four output tables and the four
# MAGIC checkpoint subfolders, then rerun. Use a separate volume/schema for each environment.
# MAGIC
# MAGIC ### Connect a real Kafka source
# MAGIC
# MAGIC Change `SOURCE_MODE` to `kafka`, replace the bootstrap server/topic, and configure
# MAGIC TLS/SASL options or Databricks service credentials for your provider. The broker must
# MAGIC be reachable from serverless compute. On Free Edition, outbound internet is restricted;
# MAGIC a local `localhost:9092` Docker broker cannot be reached from this notebook. Rerun the
# MAGIC notebook to incrementally process newly available Kafka messages.
