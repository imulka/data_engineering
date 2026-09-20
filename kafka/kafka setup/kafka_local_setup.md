# Apache Kafka Local Setup Guide

This guide documents how to install and start Apache Kafka locally on Ubuntu/Linux using Java and KRaft mode, without Docker or ZooKeeper.

## Project Location

```bash
/home/isaacm/etlworks/kafka
```

## 1. Verify Java

Kafka 4.x requires Java 17 or newer.

```bash
java --version
```

Example working version:

```text
openjdk 21.0.12
```

## 2. Create a Python Virtual Environment

From the Kafka project directory:

```bash
cd /home/isaacm/etlworks/kafka
python -m venv .venv
source .venv/bin/activate
```

Upgrade pip:

```bash
pip install --upgrade pip
```

Install pandas:

```bash
pip install pandas
```

Verify:

```bash
python -c "import pandas as pd; print(pd.__version__)"
```

## 3. Download Apache Kafka

Download Kafka:

```bash
wget https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
```

Extract it:

```bash
tar -xzf kafka_2.13-4.3.1.tgz
```

Verify:

```bash
kafka_2.13-4.3.1/bin/kafka-topics.sh --version
```

Expected:

```text
4.3.1
```

## 4. Initialize Kafka KRaft Storage

Enter the Kafka installation directory:

```bash
cd /home/isaacm/etlworks/kafka/kafka_2.13-4.3.1
```

Generate a cluster ID:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

Display it:

```bash
echo $KAFKA_CLUSTER_ID
```

Format Kafka storage:

```bash
bin/kafka-storage.sh format   --standalone   -t "$KAFKA_CLUSTER_ID"   -c config/server.properties
```

You only need to format the storage when initializing a new Kafka environment.

## 5. Start the Kafka Broker

In Terminal 1:

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```

Leave this terminal running.

Successful startup should include messages similar to:

```text
Awaiting socket connections on 0.0.0.0:9092
Kafka Server started
```

Kafka is now listening on:

```text
localhost:9092
```

## 6. Open a Second Terminal

In Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
```

Use this terminal for Kafka commands such as creating topics, listing topics, and testing producers and consumers.

---

# Restarting Kafka Later

You do **not** need to regenerate the cluster ID or reformat storage every time.

After restarting your computer, open Terminal 1 and run:

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```

Then open Terminal 2:

```bash
cd /home/isaacm/etlworks/kafka
source .venv/bin/activate
cd kafka_2.13-4.3.1
```

Kafka will again be available at:

```text
localhost:9092
```

## Stop Kafka

In the terminal running the Kafka broker, press:

```text
Ctrl+C
```

## Current ETL Project Goal

The Spotify Kafka pipeline will be:

```text
spotify_artist_streaming_2020_2025.csv
              |
              v
       Python Producer
              |
              v
   Kafka topic: spotify-streams
              |
              v
       Python Consumer
              |
              v
   Clean / Transform / Validate
              |
              v
 spotify_streams_processed.csv
```

The CSV will simulate a live streaming data source by sending individual Spotify records into Kafka one event at a time.
