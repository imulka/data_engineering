import json
import time
import argparse

import pandas as pd
from confluent_kafka import Producer


CSV_FILE = "spotify_artist_streaming_2020_2025.csv"
TOPIC = "spotify-streams"


parser = argparse.ArgumentParser()

parser.add_argument(
    "--start",
    type=int,
    default=0,
    help="Row number to start streaming from"
)

parser.add_argument(
    "--count",
    type=int,
    default=10,
    help="Number of records to stream"
)

parser.add_argument(
    "--delay",
    type=float,
    default=1.0,
    help="Delay between messages in seconds"
)

args = parser.parse_args()


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent -> topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


df = pd.read_csv(CSV_FILE)

end = args.start + args.count

records = df.iloc[args.start:end]

print(f"Loaded {len(df)} Spotify records")
print(
    f"Streaming rows {args.start} "
    f"through {end - 1}\n"
)


for _, row in records.iterrows():

    record = row.to_dict()

    record = {
        key: (
            None
            if pd.isna(value)
            else value.item()
            if hasattr(value, "item")
            else value
        )
        for key, value in record.items()
    }

    producer.produce(
        TOPIC,
        key=str(record["track_id"]),
        value=json.dumps(record),
        callback=delivery_report,
    )

    producer.poll(0)

    print(
        f"Streaming: {record['track_name']} "
        f"by {record['artist_name']}"
    )

    time.sleep(args.delay)


producer.flush()

print(f"\nFinished streaming {len(records)} Spotify records.")