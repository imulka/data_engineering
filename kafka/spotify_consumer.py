import json
import csv
import os
from confluent_kafka import Consumer


TOPIC = "spotify-streams"
OUTPUT_FILE = "spotify_streams_processed.csv"


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "spotify-etl-live",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])


fields = [
    "track_id",
    "track_name",
    "artist_name",
    "genre",
    "country",
    "popularity",
    "stream_count",
    "duration_minutes",
    "energy",
    "danceability",
    "upbeat_score",
    "stream_level"
]

processed_ids = set()

if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r", newline="") as existing_file:
        reader = csv.DictReader(existing_file)

        for row in reader:
            processed_ids.add(row["track_id"])

print(f"Loaded {len(processed_ids)} previously processed track IDs.")


file_exists = os.path.exists(OUTPUT_FILE)


with open(OUTPUT_FILE, "a", newline="") as file:

    writer = csv.DictWriter(file, fieldnames=fields)

    if not file_exists:
        writer.writeheader()

    print("Kafka ETL consumer running...")
    print("Waiting for Spotify events. Press Ctrl+C to stop.\n")

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

            if record["track_id"] in processed_ids:
                print(
                    f"Skipping duplicate: "
                    f"{record['track_name']} by {record['artist_name']}"
                )
                continue

            # TRANSFORM
            stream_count = int(record["stream_count"])

            if stream_count >= 100000:
                stream_level = "HIGH"
            elif stream_count >= 10000:
                stream_level = "MEDIUM"
            else:
                stream_level = "LOW"

            transformed = {
                "track_id": record["track_id"],
                "track_name": record["track_name"],
                "artist_name": record["artist_name"],
                "genre": record["genre"],
                "country": record["country"],
                "popularity": record["popularity"],
                "stream_count": stream_count,
                "duration_minutes": record["duration_minutes"],
                "energy": record["energy"],
                "danceability": record["danceability"],
                "upbeat_score": record["upbeat_score"],
                "stream_level": stream_level
            }

            writer.writerow(transformed)

            processed_ids.add(record["track_id"])

            # Write immediately instead of waiting for program shutdown
            file.flush()

            print(
                f"Processed: {record['track_name']} "
                f"by {record['artist_name']} "
                f"-> {stream_level}"
            )

    except KeyboardInterrupt:
        print("\nStopping Kafka consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")