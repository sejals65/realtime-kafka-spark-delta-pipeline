# Real-Time Data Processing Pipeline — Kafka + Spark

[![Tests](https://github.com/sejals65/realtime-kafka-spark-delta-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/sejals65/realtime-kafka-spark-delta-pipeline/actions/workflows/tests.yml)

A local reference implementation for continuous event processing with Kafka, Spark Structured Streaming, PySpark, and Delta Lake. It uses three independently restartable jobs and Delta tables for the Bronze → Silver → Gold data flow. For a guided walkthrough of one message from input to output, see [BEGINNER_GUIDE.md](BEGINNER_GUIDE.md).

## Architecture

```text
event producers
      │ JSON (key = event_id)
      ▼
Kafka: events (partitioned topic; key-based ordering)
      │ at-least-once source + Kafka offsets in Spark checkpoint
      ▼
Bronze Delta: raw JSON + Kafka topic/partition/offset + ingestion time
      │ incremental Delta stream
      ├── Silver Delta: parsed, validated, event_id deduplicated, event_date partitioned
      └── Silver quarantine Delta: rejected records + quality_error
                │ incremental Delta stream
                ▼
Gold Delta: five-minute product purchase counts, units, and gross revenue
```

Bronze preserves the source payload and Kafka coordinates for replay and audit. Silver normalizes fields, applies the rules in `quality_rules`, keeps invalid rows in quarantine, and deduplicates valid events using a stateful watermark. Gold emits completed event-time windows in append mode. Each streaming query has its own durable checkpoint; keep those directories when restarting. The local compose file mounts data and checkpoints so container replacement does not discard progress.

## Start locally

Requires Docker Compose and a host with enough memory for Kafka and Spark.

```sh
docker compose up -d kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic events --partitions 6 --replication-factor 1
```

Start one Spark job per terminal. Start Bronze first, then start Silver after the Bronze Delta table appears under `data/bronze/events`; start Gold after Silver creates `data/silver/events`. Keeping each layer in its own process makes failures and scaling independent:

```sh
PIPELINE_MODE=bronze docker compose run --rm spark
PIPELINE_MODE=silver docker compose run --rm spark
PIPELINE_MODE=gold docker compose run --rm spark
```

Because the compose service defaults to `bronze`, the environment variable is passed through to the container. To submit from a host Python environment instead, install `pip install -e .` and run `./entrypoint.sh bronze` (or `silver` / `gold`) with `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`.

Send a sample event from the host:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic events --property parse.key=true \
  --property key.separator=:
```

Then enter one JSON line per event, using `event_id` as the key:

```text
evt-1001:{"event_id":"evt-1001","event_type":"purchase","user_id":"u-42","product_id":"sku-7","quantity":2,"amount":39.98,"event_ts":"2026-11-03T04:00:00Z"}
```

The Kafka topic should be partitioned by a stable key such as `event_id` or `user_id`; producers should set the Kafka message key so events with the same key retain partition ordering. Increase topic partitions and Spark executor capacity as throughput grows. Set `MAX_OFFSETS_PER_TRIGGER` and `SHUFFLE_PARTITIONS` to control per-trigger input and processing parallelism.

## Data contract and quality

The JSON contract is defined in `src/pipeline.py` (`EVENT_SCHEMA`). `event_id`, `event_type`, `user_id`, and `event_ts` are required and must not be blank. Supported event types are `view`, `cart`, `purchase`, and `refund`; quantity must be positive when supplied, and amount must be nonnegative when supplied. Unknown/malformed timestamps and missing required fields are routed to `silver/quarantine` with a reason. Add contract changes with a deliberate schema migration; Bronze retains the original JSON to support reprocessing.

Run the unit tests with `python -m unittest discover -s tests -v` after installing the project dependencies. GitHub Actions runs the same checks for pushes and pull requests.

Delta tables are written under `data/{bronze,silver,gold}`. Query-specific offsets/state live under `checkpoints/{bronze,silver,quarantine,gold}`. Do not reuse a checkpoint across query definitions or delete it during routine recovery. `startingOffsets` only applies when the Bronze checkpoint is first created. The watermark bounds Silver dedup state; events arriving later than the configured delay may not be deduplicated against expired state. Gold uses event time and emits windows after its watermark delay.

## Production hardening

The compose stack is a single-broker development setup. For production, use a managed or replicated Kafka cluster, secure TLS/SASL listeners and secrets, durable shared storage for Delta and checkpoints (for example object storage with a supported catalog), multiple Spark workers, monitoring/alerts for query progress and lag, and a tested retention/compaction/Delta vacuum policy. Size partitions, executor cores, and trigger interval from measured event volume and latency targets. Delta sink writes are idempotent under Spark checkpoint recovery; source-to-sink processing is not a single Kafka/Delta transaction, so end-to-end semantics depend on preserving Spark checkpoints and Delta transaction history.
