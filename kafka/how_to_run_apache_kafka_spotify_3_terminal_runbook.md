# Apache Kafka Spotify ETL — 3-Terminal Runbook

Use this guide **after Kafka has already been installed and initialized**.

Project directory:

```bash
/home/isaacm/etlworks/kafka
```

Kafka installation:

```bash
/home/isaacm/etlworks/kafka/kafka_2.13-4.3.1
```

Python virtual environment:

```bash
/home/isaacm/etlworks/kafka/.venv
```

Kafka topic:

```text
spotify-streams
```

SQLite database:

```text
spotify_streams.db
```

---

# Pipeline

```text
spotify_artist_streaming_2020_2025.csv
                    |
                    v
          spotify_producer.py
                    |
                    v
        Kafka topic: spotify-streams
             (3 partitions)
                    |
                    v
      spotify_consumer_sqlite.py
                    |
          transform / classify
                    |
                    v
          spotify_streams.db
```

The producer simulates a live Spotify event stream by reading rows from the CSV and publishing them to Kafka one at a time.

---

# Terminal 1 — Start Kafka Broker

Open Terminal 1.

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
```

Start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Leave Terminal 1 running.

Successful startup should contain something similar to:

```text
Awaiting socket connections on 0.0.0.0:9092
Kafka Server started
```

Kafka is now available at:

```text
localhost:9092
```

## Important

Do **not** run the KRaft formatting command again during normal startup.

The following commands were only needed when Kafka was initially installed:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

Do not repeat them unless deliberately creating a new Kafka storage environment.

---

# Terminal 2 — Producer and Kafka Administration

Open Terminal 2.

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
```

This terminal is used to:

- publish Spotify records;
- inspect Kafka topics;
- inspect consumer groups;
- check consumer lag;
- query SQLite.

---

# Terminal 3 — Kafka ETL Consumer

Open Terminal 3.

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
```

Start the SQLite ETL consumer:

```bash
python spotify_consumer_sqlite.py
```

Expected:

```text
Kafka -> SQLite consumer running...
Press Ctrl+C to stop.
```

Leave Terminal 3 running.

The consumer:

1. reads Kafka events;
2. parses JSON;
3. transforms `stream_count`;
4. creates a `stream_level`;
5. writes the record to SQLite;
6. rejects duplicate `track_id` values.

Stream classification:

```text
100,000+ streams       -> HIGH
10,000 - 99,999        -> MEDIUM
below 10,000           -> LOW
```

SQLite uses:

```sql
track_id TEXT PRIMARY KEY
```

and the consumer uses:

```sql
INSERT OR IGNORE
```

so duplicate tracks are not inserted again.

---

# Run the Streaming Exercise

With:

- Terminal 1 = Kafka running
- Terminal 3 = consumer running

use Terminal 2 to publish Spotify records.

## Send 10 records

```bash
cd /home/isaacm/etlworks/kafka

python spotify_producer.py \
  --start 151 \
  --count 10 \
  --delay 0.5
```

Meaning:

```text
--start 151   start at CSV row 151
--count 10    publish 10 events
--delay 0.5   wait 0.5 seconds between events
```

Terminal 2 will show messages such as:

```text
Streaming: Song Name by Artist
Sent -> topic=spotify-streams partition=1 offset=...
```

Terminal 3 should immediately show:

```text
Inserted: Song Name by Artist -> MEDIUM
```

or, if already stored:

```text
Skipped duplicate: Song Name by Artist
```

---

# Stream a Larger Batch

Example: send 100 records:

```bash
python spotify_producer.py \
  --start 200 \
  --count 100 \
  --delay 0.1
```

This simulates a faster live event stream.

---

# Check the Kafka Topic

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka/kafka_2.13-4.3.1
```

List topics:

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
spotify-streams
```

Describe the Spotify topic:

```bash
bin/kafka-topics.sh \
  --describe \
  --topic spotify-streams \
  --bootstrap-server localhost:9092
```

The topic currently has:

```text
PartitionCount: 3
ReplicationFactor: 1
```

Because this is a single-machine exercise, replication factor 1 is expected.

---

# Check Consumer Lag

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka/kafka_2.13-4.3.1
```

Run:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group spotify-sqlite-consumer
```

Important columns:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

Conceptually:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Interpretation:

```text
LAG = 0    consumer is caught up
LAG > 0    messages are waiting to be processed
```

---

# Demonstrate Kafka Durability / Recovery

This is a useful interview demonstration.

## 1. Stop the consumer

In Terminal 3:

```text
Ctrl+C
```

Do **not** stop Kafka in Terminal 1.

## 2. Produce events while the consumer is offline

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka

python spotify_producer.py \
  --start 300 \
  --count 10 \
  --delay 0.2
```

Kafka accepts and stores the messages even though the ETL consumer is offline.

## 3. Check lag

```bash
cd /home/isaacm/etlworks/kafka/kafka_2.13-4.3.1

bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group spotify-sqlite-consumer
```

You should now see:

```text
LAG > 0
```

## 4. Restart the consumer

In Terminal 3:

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
python spotify_consumer_sqlite.py
```

The consumer should immediately process the queued messages.

## 5. Check lag again

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka/kafka_2.13-4.3.1

bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group spotify-sqlite-consumer
```

Expected after processing:

```text
LAG = 0
```

This demonstrates that Kafka retained the data while the consumer was offline and that the consumer resumed from its stored offsets.

---

# Query SQLite

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka
```

## Count records

```bash
python - <<'PY'
import sqlite3

conn = sqlite3.connect("spotify_streams.db")
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM spotify_streams")

print("Rows in SQLite:", cursor.fetchone()[0])

conn.close()
PY
```

---

# Analytics — Stream Levels

```bash
python - <<'PY'
import sqlite3

conn = sqlite3.connect("spotify_streams.db")
cursor = conn.cursor()

cursor.execute("""
SELECT
    stream_level,
    COUNT(*) AS track_count,
    ROUND(AVG(stream_count), 2) AS avg_streams
FROM spotify_streams
GROUP BY stream_level
ORDER BY avg_streams DESC
""")

for row in cursor.fetchall():
    print(row)

conn.close()
PY
```

Example output format:

```text
('HIGH', ..., ...)
('MEDIUM', ..., ...)
('LOW', ..., ...)
```

---

# Analytics — Top Artists by Streams

```bash
python - <<'PY'
import sqlite3

conn = sqlite3.connect("spotify_streams.db")
cursor = conn.cursor()

cursor.execute("""
SELECT
    artist_name,
    COUNT(*) AS track_count,
    SUM(stream_count) AS total_streams,
    ROUND(AVG(stream_count), 2) AS avg_streams
FROM spotify_streams
GROUP BY artist_name
ORDER BY total_streams DESC
LIMIT 10
""")

for row in cursor.fetchall():
    print(row)

conn.close()
PY
```

---

# Kafka Partition Concept

The topic has three partitions:

```text
spotify-streams
├── Partition 0
├── Partition 1
└── Partition 2
```

The producer uses:

```python
key=str(record["track_id"])
```

Kafka hashes the key and chooses a partition.

Partitions allow Kafka consumers to process data in parallel.

---

# Consumer Group Concept

The SQLite consumer uses:

```text
spotify-sqlite-consumer
```

Kafka stores offsets for this consumer group.

This allows the consumer to:

```text
stop
  ↓
Kafka continues storing events
  ↓
consumer restarts
  ↓
resume from saved offsets
```

Consumers in the **same consumer group** divide partitions among themselves.

Consumers in **different consumer groups** can independently read the same topic.

---

# Quick Start — Next Time

If you only want the commands needed to restart the exercise:

## Terminal 1

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```

## Terminal 2

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
```

Then send events:

```bash
python spotify_producer.py \
  --start 400 \
  --count 10 \
  --delay 0.5
```

## Terminal 3

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
python spotify_consumer_sqlite.py
```

Recommended order:

```text
1. Terminal 1 -> start Kafka
2. Terminal 3 -> start consumer
3. Terminal 2 -> start producer
```

---

# Clean Shutdown

## Stop the consumer

Terminal 3:

```text
Ctrl+C
```

## Stop Kafka

Terminal 1:

```text
Ctrl+C
```

Terminal 2 can simply be closed after producer/admin commands finish.

---

# Files Used in This Exercise

```text
/home/isaacm/etlworks/kafka/
│
├── .venv/
├── kafka_2.13-4.3.1/
├── spotify_artist_streaming_2020_2025.csv
├── spotify_producer.py
├── spotify_consumer.py
├── spotify_consumer_sqlite.py
├── spotify_streams_processed.csv
└── spotify_streams.db
```

---

# Interview Summary

> I built a local Apache Kafka streaming ETL pipeline using Python. A producer reads Spotify records and publishes JSON events to a Kafka topic with three partitions. A consumer group reads those events, transforms streaming metrics, classifies stream volume, and persists the results into SQLite with idempotent inserts. I also tested partition-based parallelism, consumer offsets, consumer lag, and recovery by stopping the consumer, continuing to publish events, and verifying that Kafka retained the backlog and the consumer resumed from its saved offsets.
