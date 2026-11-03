"""Print the current Silver, quarantine, and Gold Delta results concisely."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

# Make the host setup work from this project even when its directory contains spaces.
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
os.environ.setdefault("SPARK_HOME", str(Path(__import__("pyspark").__file__).resolve().parent))
os.environ.setdefault("PYSPARK_PYTHON", "python3.11")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def main() -> None:
    data_dir = Path(os.environ.get("DATA_DIR", PROJECT_DIR / "data"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("view-pipeline-results")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    ivy_jars = Path.home() / ".ivy2/jars"
    delta_jars = [
        ivy_jars / "io.delta_delta-spark_2.12-3.2.0.jar",
        ivy_jars / "io.delta_delta-storage-3.2.0.jar",
        ivy_jars / "org.antlr_antlr4-runtime-4.9.3.jar",
    ]
    if all(jar.exists() for jar in delta_jars):
        # Reuse jars resolved by the running pipeline so viewing results stays quiet.
        spark = builder.config("spark.jars", ",".join(map(str, delta_jars))).getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    tables = [
        (
            "CLEAN EVENTS (Silver)",
            data_dir / "silver/events",
            ["event_id", "event_type", "user_id", "product_id", "quantity", "amount", "event_ts"],
        ),
        (
            "REJECTED EVENTS (Quarantine)",
            data_dir / "silver/quarantine",
            ["event_id", "event_type", "quality_error"],
        ),
        (
            "SALES TOTALS (Gold)",
            data_dir / "gold/product_sales_5m",
            ["window_start", "product_id", "purchase_count", "gross_revenue", "units_sold"],
        ),
    ]

    try:
        for title, path, columns in tables:
            print(f"\n=== {title} ===")
            if not path.exists():
                print("No table yet. Start the streaming jobs first.")
                continue
            df = spark.read.format("delta").load(str(path))
            available = [column for column in columns if column in df.columns]
            if df.limit(1).count() == 0:
                print("No rows yet.")
            else:
                df.select(*available).orderBy(available[0]).show(30, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
