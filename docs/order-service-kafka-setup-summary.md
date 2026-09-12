# Order Service & Kafka Producer-Consumer Setup — Full Summary

A complete record of building and debugging the first working piece of ResilienceGuard: the Order Service publishing events to Kafka.

---

## 1. What We Built

A Spring Boot microservice (`order-service`) that:
1. Exposes a REST endpoint (`POST /orders`) to accept a new order
2. Publishes that order as a JSON event to a Kafka topic (`order-placed`)
3. Was verified end-to-end by confirming the message actually lands in Kafka

This is the foundation of the event-driven pipeline (Order → Payment → Notification) at the core of ResilienceGuard.

---

## 2. Why Kafka as the Event Backbone

Before writing any code, this was captured as an Architecture Decision Record (ADR 001). The reasoning:

- **Replay capability** — Kafka retains events as a durable log rather than deleting them once consumed. If a consumer service crashes (including during a deliberate chaos test), it can resume from where it left off instead of losing data.
- **Ordering guarantees** — Kafka preserves message order within a partition, which matters for a sequential flow like order → payment → notification.
- **Audit trail by design** — the log-based retention model naturally supports the project's DORA-aligned goal of being able to reconstruct what happened and when, without extra tooling.

*(See Section 7 for the full Kafka vs RabbitMQ comparison.)*

---

## 3. Kafka Setup via Docker

### 3.1 Why Docker for Kafka
Kafka requires a broker process and (in the version used here) a Zookeeper coordination service. Running these as isolated containers avoids installing Java-based services directly on the host machine and mirrors how these are typically deployed in real environments.

### 3.2 The docker-compose file

```bash
cd ~/projects/resilienceguard

cat > docker-compose.kafka.yml << 'EOF'
version: '3.8'
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
EOF
```

### 3.3 Start and verify

```bash
docker compose -f docker-compose.kafka.yml up -d
docker ps
```

Expected: two containers running — `resilienceguard-kafka-1` and `resilienceguard-zookeeper-1`.

### 3.4 Create a topic and confirm messages flow

```bash
# Create a test topic
docker exec -it resilienceguard-kafka-1 kafka-topics \
  --create --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Producer (Tab A)
docker exec -it resilienceguard-kafka-1 kafka-console-producer \
  --topic test-topic --bootstrap-server localhost:9092

# Consumer (Tab B)
docker exec -it resilienceguard-kafka-1 kafka-console-consumer \
  --topic test-topic --bootstrap-server localhost:9092 --from-beginning
```

Typing a message in Tab A and seeing it appear in Tab B confirmed Kafka was fully operational before any application code was written — de-risking the newest technology in isolation first.

### 3.5 Create the application topic

```bash
docker exec -it resilienceguard-kafka-1 kafka-topics \
  --create --topic order-placed \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

---

## 4. Scaffolding the Order Service

### 4.1 Generate the project

```bash
cd ~/projects/resilienceguard

curl https://start.spring.io/starter.zip \
  -d type=maven-project \
  -d language=java \
  -d baseDir=order-service \
  -d groupId=com.resilienceguard \
  -d artifactId=order-service \
  -d name=order-service \
  -d packageName=com.resilienceguard.orderservice \
  -d javaVersion=21 \
  -d dependencies=web,kafka,actuator,devtools,lombok \
  -o order-service.zip

unzip order-service.zip -d order-service
rm order-service.zip
cd order-service
```

> **Note:** `bootVersion` was intentionally omitted after an initial attempt with `3.3.0` failed — Spring Initializr now requires Spring Boot ≥4.0.0. Omitting the parameter defaults to the current stable release (4.1.1 in this build).

### 4.2 Fix a nested-folder extraction issue
Because `baseDir=order-service` was specified inside a zip that was also extracted into a folder named `order-service`, the result was a doubled path (`order-service/order-service/...`). Fixed by flattening:

```bash
mv order-service/.gitignore order-service/.gitattributes order-service/.mvn . 2>/dev/null
mv order-service/* .
rmdir order-service
```

### 4.3 Application configuration

```bash
cat > src/main/resources/application.yml << 'EOF'
server:
  port: 8081

spring:
  application:
    name: order-service
  kafka:
    bootstrap-servers: localhost:9092

management:
  endpoints:
    web:
      exposure:
        include: health,info
EOF

# Removed the auto-generated application.properties to avoid config duplication/conflict
rm src/main/resources/application.properties
```

### 4.4 Application code

```bash
mkdir -p src/main/java/com/resilienceguard/orderservice/{controller,model,service,config}
```

**Order model** — represents the event payload:
```java
package com.resilienceguard.orderservice.model;

import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Data
public class Order {
    private String orderId = UUID.randomUUID().toString();
    private String customerId;
    private double amount;
    private Instant createdAt = Instant.now();
}
```

**Kafka producer configuration:**
```java
package com.resilienceguard.orderservice.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

**Producer service** — publishes to the topic:
```java
package com.resilienceguard.orderservice.service;

import com.resilienceguard.orderservice.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrder(Order order) {
        kafkaTemplate.send("order-placed", order.getOrderId(), order);
    }
}
```

**REST controller** — entry point:
```java
package com.resilienceguard.orderservice.controller;

import com.resilienceguard.orderservice.model.Order;
import com.resilienceguard.orderservice.service.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderProducer orderProducer;

    @PostMapping
    public Order createOrder(@RequestBody Order order) {
        orderProducer.publishOrder(order);
        return order;
    }
}
```

---

## 5. Debugging Log — Real Issues Hit and Fixed

These weren't scripted — each was a genuine runtime failure, diagnosed from the stack trace and fixed. Good material for interview "what went wrong" discussions.

### Issue 1: Port already in use
```
Web server failed to start. Port 8081 was already in use.
```
**Cause:** the app was already running in another terminal tab; a second `./mvnw spring-boot:run` collided with it.
**Fix:**
```bash
lsof -i :8081
kill -9 <PID>
```
**Lesson:** only run one instance of the app at a time; use separate tabs only for `curl`/Kafka CLI commands, not for a second server process.

### Issue 2: `ClassNotFoundException: com.fasterxml.jackson.core.type.TypeReference`
**Cause:** `JsonSerializer` (used by the Kafka producer to convert Java objects to JSON) depends on Jackson's databind library, which wasn't declared as a dependency.
**Fix:** added to `pom.xml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

### Issue 3: `InvalidDefinitionException: Java 8 date/time type java.time.Instant not supported`
**Cause:** Jackson doesn't serialize `java.time.Instant` (and other Java 8 date/time types) without an additional module.
**Fix:** added to `pom.xml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
</dependency>
```

After both fixes, restarting the app (`Ctrl+C` then `./mvnw spring-boot:run`) resolved the issue completely.

### Known follow-up (not yet applied)
`createdAt` currently serializes as a raw epoch number (e.g. `1789214633.750164000`) rather than an ISO timestamp, because `jackson-datatype-jsr310` defaults to timestamp format. Since RTO calculation later depends on readable/comparable timestamps, this can be switched with:
```yaml
spring:
  jackson:
    serialization:
      write-dates-as-timestamps: false
```

---

## 6. End-to-End Verification

**Start the app:**
```bash
cd ~/projects/resilienceguard/order-service
./mvnw spring-boot:run
```

**Send a test request (separate terminal tab):**
```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-123", "amount": 250.00}'
```

**Confirm the message landed in Kafka:**
```bash
docker exec -it resilienceguard-kafka-1 kafka-console-consumer \
  --topic order-placed --bootstrap-server localhost:9092 --from-beginning
```

**Result achieved:**
```json
{"orderId":"23cbe91a-67ea-4e3b-95e8-e65c560b3f71","customerId":"cust-123","amount":250.0,"createdAt":1789214633.750164000}
```

This confirmed the full path: REST request → serialization → Kafka producer → topic → visible to a consumer.

---

## 7. Kafka vs RabbitMQ — Why Kafka Was Used Throughout

### Why not RabbitMQ for this project
Using two different message brokers (Kafka for some services, RabbitMQ for others) without a specific technical reason is usually a sign of inconsistent legacy decisions rather than intentional design. For ResilienceGuard, one broker was chosen deliberately and used consistently:

| Requirement | Kafka | RabbitMQ |
|---|---|---|
| **Event replay after consumer failure** | Native — consumers track offsets in a durable log and can reprocess from any point | Not native — once a message is acknowledged, it's gone unless extra durability is engineered |
| **Ordering guarantees** | Preserved within a partition | Possible but requires careful queue/consumer design |
| **Audit trail** | Log-based retention naturally supports reconstructing history — directly relevant to this project's DORA-aligned auditability goal | Queues are transient by design; would need additional logging infrastructure to match this |
| **Chaos-testing fit** | A killed consumer can resume from its last committed offset — ideal for simulating and recovering from failure | Requires manual ack/redelivery design to achieve similar guarantees |

### When RabbitMQ genuinely is the better choice
To be fair, RabbitMQ isn't inferior — it's suited to different problems:
- **Complex routing** — topic exchanges, fanout patterns, priority queues
- **Low-latency request/reply (RPC-style) messaging** — where a service needs a direct, synchronous-feeling reply, not just an event
- **Simpler operational footprint for smaller systems** — RabbitMQ can be lighter to run and reason about when Kafka's log-retention model isn't needed
- **Task-queue-style workloads** — e.g., background job processing where messages should be consumed once and discarded, not retained for replay

### The decision for this project
Payment Service (and Notification Service) consume the same event stream that Order Service produces, and the project's core value proposition — auditable, replayable incident history — depends on Kafka's retention model. RabbitMQ was evaluated and intentionally not used; this is documented as ADR 001 rather than left as an unstated assumption, since a deliberate "we didn't need X and here's why" is a stronger architectural signal than defaulting to whatever's already running.
