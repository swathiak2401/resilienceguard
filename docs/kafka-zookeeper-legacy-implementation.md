# Kafka (Zookeeper Mode) — Implementation Record

Documentation of the Kafka architecture used in the initial build of ResilienceGuard, before migrating to KRaft mode. Preserved here as a reference for what was built, why it worked the way it did, and how the pieces coordinated.

---

## 1. Why This Record Exists

The initial Kafka setup used **Zookeeper mode** — the architecture that shipped as Kafka's default for most of its history, via the `confluentinc/cp-kafka:7.6.0` image. This was not a deliberate architectural choice at the time; it was the default behavior of the Docker image used. Apache Kafka 4.0 (released March 2025) removed Zookeeper support entirely, making **KRaft mode** the only supported architecture going forward.

This document captures how the Zookeeper-based setup worked before it is replaced, both as a record of what was actually built and tested, and because understanding this legacy architecture remains directly relevant — many production Kafka clusters, particularly at large, slower-moving organizations, have not yet migrated off it.

---

## 2. Architecture Overview

Two separate processes were run as Docker containers, coordinating over an internal Docker network:

| Component | Role |
|---|---|
| **Zookeeper** | Coordination service. Stores cluster metadata: which broker is the controller, topic configuration, partition leader assignment, access control lists. |
| **Kafka broker** | Message store and traffic handler. Owns producing, storing, and serving messages. Asks Zookeeper for metadata decisions rather than storing them itself. |

The key architectural point: **the broker does not make coordination decisions independently**. Every time the broker needed to know things like "who is the controller" or "which broker leads this partition," it queried Zookeeper rather than resolving this internally. This is precisely what KRaft mode later eliminated — in KRaft, this metadata moved into an internal Kafka topic managed by a Raft consensus quorum, removing the need for a second system entirely.

---

## 3. The docker-compose Definition

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

**What each setting did:**

| Setting | Purpose |
|---|---|
| `ZOOKEEPER_CLIENT_PORT` | Port Zookeeper listens on for connections from Kafka brokers |
| `KAFKA_ZOOKEEPER_CONNECT` | Told the broker where to find Zookeeper — `zookeeper:2181`, resolved via the Docker Compose internal network by service name |
| `KAFKA_BROKER_ID` | Unique identifier for this broker within the cluster (only one broker existed here, but Kafka requires an ID regardless) |
| `KAFKA_ADVERTISED_LISTENERS` | The address the broker tells connecting clients to use — `localhost:9092`, so applications on the host machine (like Order Service) could reach it |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Set to `1` because this was a single-broker setup with no redundancy |

`depends_on` ensured Zookeeper started first — the Kafka broker cannot initialize without successfully connecting to Zookeeper on startup, since it needs to register itself and read initial cluster state before it can begin accepting producer/consumer connections.

---

## 4. Step-by-Step: What Happened on Startup

1. **Zookeeper container starts.** It begins listening on port 2181 and initializes its internal data store, ready to track cluster metadata.
2. **Kafka broker container starts**, connects to `zookeeper:2181`, and registers itself as broker ID 1.
3. **The broker asks Zookeeper**: "am I the controller?" With only one broker, it is automatically elected controller — the broker responsible for managing partition leadership and reacting to broker failures.
4. **The broker becomes ready** to accept connections on port 9092, advertised to clients as `localhost:9092`.

At this point, no topics existed yet — the cluster was live but empty.

---

## 5. Step-by-Step: Creating a Topic

```bash
docker exec -it resilienceguard-kafka-1 kafka-topics \
  --create --topic order-placed \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

What happened internally:

1. The `kafka-topics` CLI tool connected to the broker on port 9092 (not directly to Zookeeper — client tools talk to the broker, which in turn coordinates with Zookeeper on the client's behalf).
2. The broker created the topic metadata (name, partition count, replication factor) and **wrote this metadata to Zookeeper**, since topic configuration is coordination-plane data.
3. The broker then created the actual partition log — an empty append-only file on its local disk — for partition 0 of `order-placed`.
4. Because replication factor was `1`, no copies of this partition were created on other brokers (there were none to create copies on).

From this point, `order-placed` existed as a single-partition, single-copy log, ready to accept messages.

---

## 6. Step-by-Step: Producing a Message

When the Order Service published a message via `KafkaTemplate.send("order-placed", orderId, order)`:

1. The Spring Kafka producer opened a TCP connection directly to the broker at `localhost:9092` — **not** to Zookeeper. Producers never talk to Zookeeper directly.
2. The producer asked the broker (using metadata it had already cached from an earlier request): "who is the leader for partition 0 of `order-placed`?" With one broker, the answer was always itself.
3. The `Order` object was serialized to JSON bytes by `JsonSerializer`.
4. The serialized bytes, along with the message key (`orderId`), were sent to the broker.
5. The broker appended the message to the end of partition 0's log file, assigning it the next sequential **offset**.
6. The broker sent an acknowledgment back to the producer confirming the write succeeded.

Zookeeper played no role in this specific step — once the broker knew it was the leader (established at startup and cached), producing and consuming messages happened entirely through direct broker communication. Zookeeper's involvement was limited to coordination-plane events: startup, topic creation, leader election, and broker failure handling.

---

## 7. Step-by-Step: Consuming a Message

```bash
docker exec -it resilienceguard-kafka-1 kafka-console-consumer \
  --topic order-placed --bootstrap-server localhost:9092 --from-beginning
```

1. The consumer connected directly to the broker on port 9092.
2. It requested the broker's metadata for `order-placed` to find which broker held partition 0 (again, itself in this single-broker setup).
3. With `--from-beginning`, the consumer requested messages starting at offset 0.
4. The broker read the requested range from the partition's log file on disk and streamed the messages back.
5. The consumer printed each message payload to the terminal as it arrived.

No offsets were committed to a persistent consumer group in this manual CLI test — running the same command again with `--from-beginning` would replay every message from the start, since no group tracking was involved.

---

## 8. Where Zookeeper Actually Mattered

To be precise about Zookeeper's actual scope of responsibility in this setup, since it's easy to overstate:

**Zookeeper was involved in:**
- Broker registration on startup
- Controller election (which broker manages partition leadership decisions)
- Storing topic configuration (partition count, replication factor)
- Tracking partition leader/in-sync-replica (ISR) state
- Detecting broker failure (via session heartbeats) and triggering leader re-election

**Zookeeper was *not* involved in:**
- Producers sending messages
- Consumers reading messages
- Actual message storage (this lives entirely on the broker's disk)
- Message ordering or offset assignment within a partition

This distinction matters for understanding why KRaft's removal of Zookeeper doesn't change how producers/consumers work day-to-day — it changes where and how the *coordination* metadata is stored, not the data plane.

---

## 9. Known Limitations of This Setup

- **Single point of failure**: one broker, one Zookeeper node — no redundancy at either layer. A production cluster would run at least 3 Zookeeper nodes (an ensemble) and multiple brokers.
- **No security configured**: plaintext listener (`PLAINTEXT://localhost:9092`), no authentication or encryption — acceptable for local development only.
- **Manual offset management**: the CLI consumer used for testing did not use a persistent consumer group, so it could not resume from where it left off across runs.

---

## 10. Migration Note

This architecture is being replaced with **KRaft mode**, which removes the Zookeeper container entirely and has the Kafka broker manage cluster metadata internally via a Raft-based controller quorum, stored in an internal `__cluster_metadata` topic rather than an external system. The producer and consumer code (Order Service's `KafkaProducerConfig`, `OrderProducer`) requires no changes for this migration — KRaft is a metadata-layer change, not a data-plane or client-API change.
