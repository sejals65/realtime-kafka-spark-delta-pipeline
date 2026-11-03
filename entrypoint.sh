#!/bin/sh
set -eu

MODE="${1:-${PIPELINE_MODE:-bronze}}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-8}" \
  --conf spark.ui.showConsoleProgress=false \
  "$SCRIPT_DIR/src/pipeline.py" --mode "$MODE"
