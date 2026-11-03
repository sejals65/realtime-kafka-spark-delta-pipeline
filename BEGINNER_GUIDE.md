# The pipeline, in plain language

Think of Kafka as an inbox for events. Spark watches that inbox, saves every message, cleans it, and keeps useful totals. Delta Lake is the set of tables on disk where those results are saved.

```text
your JSON message
      ↓
Kafka topic `events` (the inbox)
      ↓
Bronze (original message + where Kafka stored it)
      ↓
Silver (clean rows) ── bad rows → quarantine
      ↓
Gold (small business summary: purchases and sales)
```

## 1. The input

This is one purchase event. It says user `u-42` bought two of `sku-7` for `$39.98` at 03:00 UTC:

```json
{
  "event_id": "evt-1001",
  "event_type": "purchase",
  "user_id": "u-42",
  "product_id": "sku-7",
  "quantity": 2,
  "amount": 39.98,
  "event_ts": "2026-11-03T03:00:00Z"
}
```

The producer sends that JSON to Kafka topic `events`. A **topic** is like an inbox named for one kind of message. Kafka splits a topic into **partitions**, which are lanes that let many messages be stored and read in parallel. The producer uses `event_id` as the message key; Kafka also records a partition and offset (the message's position in that partition).

## 2. What each layer does

- **Bronze means “keep the receipt.”** `src/pipeline.py` copies the JSON as-is into `data/bronze/events` and adds Kafka's topic, partition, offset, and the time Spark received it. If something needs investigation, the original input is still available.
- **Silver means “check and clean it.”** Spark reads Bronze, turns the JSON text into named columns, and checks required fields and allowed values. A good event goes into `data/silver/events`. A bad one goes into `data/silver/quarantine` with a reason such as `invalid_event_type`. Repeated `event_id` values are deduplicated within the configured two-hour watermark.
- **Gold means “answer a useful question.”** Spark keeps purchase events and groups them by product and five-minute event-time window. The result is written to `data/gold/product_sales_5m`.

For the example above, the Bronze row still contains the JSON text. In Silver, the same information is easier to query as columns, for example `event_id=evt-1001`, `event_type=purchase`, `quantity=2`, `amount=39.98`, `is_valid=true`. Gold summarizes that row as one purchase, two units, and `$39.98` for `sku-7`.

Gold waits for its event-time window to be complete before writing it. The configured two-hour watermark allows late events to arrive. A later purchase event advances that watermark; a later view event does not, because Gold filters to purchases first. This is why a recent purchase may appear in Silver before its Gold total appears.

## 3. See the current results without the Spark log flood

From the project folder, run:

```sh
.venv/bin/python3.11 view_results.py
```

It prints only the useful columns from Silver, quarantine, and Gold. The Delta files themselves are Parquet data and are not meant to be read as plain text. The viewer uses Spark to display them as small tables.

To inspect Kafka's inbox instead, these commands show topic setup and the first three messages:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  /opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic events

JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  /opt/homebrew/bin/kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic events --from-beginning --property print.key=true --max-messages 3
```

## 4. What the main files are for

- `src/pipeline.py` contains the three Spark jobs and the event checks.
- `entrypoint.sh` starts one of those jobs (`bronze`, `silver`, or `gold`).
- `docker-compose.yml` and `Dockerfile` describe the optional container-based local setup.
- `tests/test_pipeline.py` checks examples of good and bad events.
- `data/` contains the visible Bronze, Silver, quarantine, and Gold tables.
- `checkpoints/` contains Spark's saved reading position and deduplication state. Keeping it lets a restarted job continue where it stopped.

## 5. Why there are three running jobs

Bronze, Silver, and Gold each run as a separate Spark process. That lets each stage restart independently. The word **streaming** means each process stays open and checks for more data repeatedly; it does not mean the terminal must keep printing messages. Routine Spark logs are now reduced to warnings. For everyday inspection, use `view_results.py` instead of reading the Spark job logs.
