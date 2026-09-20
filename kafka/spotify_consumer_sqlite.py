import json
import sqlite3
from confluent_kafka import Consumer

TOPIC = "spotify-streams"
DB_FILE = "spotify_streams.db"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "spotify-sqlite-consumer",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS spotify_streams (
    track_id TEXT PRIMARY KEY,
    track_name TEXT,
    artist_name TEXT,
    genre TEXT,
    country TEXT,
    popularity INTEGER,
    stream_count INTEGER,
    duration_minutes REAL,
    energy REAL,
    danceability REAL,
    upbeat_score REAL,
    stream_level TEXT
)
""")

conn.commit()

print("Kafka -> SQLite consumer running...")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))

        if "track_id" not in record:
            continue

        stream_count = int(record["stream_count"])

        if stream_count >= 100000:
            stream_level = "HIGH"
        elif stream_count >= 10000:
            stream_level = "MEDIUM"
        else:
            stream_level = "LOW"

        cursor.execute("""
        INSERT OR IGNORE INTO spotify_streams (
            track_id,
            track_name,
            artist_name,
            genre,
            country,
            popularity,
            stream_count,
            duration_minutes,
            energy,
            danceability,
            upbeat_score,
            stream_level
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record["track_id"],
            record["track_name"],
            record["artist_name"],
            record["genre"],
            record["country"],
            record["popularity"],
            stream_count,
            record["duration_minutes"],
            record["energy"],
            record["danceability"],
            record["upbeat_score"],
            stream_level
        ))

        conn.commit()

        if cursor.rowcount == 1:
            print(
                f"Inserted: {record['track_name']} "
                f"by {record['artist_name']} -> {stream_level}"
            )
        else:
            print(
                f"Skipped duplicate: {record['track_name']} "
                f"by {record['artist_name']}"
            )

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    conn.close()
    print("Consumer and SQLite connection closed.")