# ResilienceGuard

**DORA-Aligned Chaos Engineering & Operational Resilience Platform**

ResilienceGuard simulates a Kafka-connected payment processing pipeline (Order → Payment → Notification) and deliberately injects controlled failures into it — pod kills, network delays, CPU throttling — to measure how quickly and reliably the system detects and recovers.

The platform is built around a real, current regulatory driver: the EU's Digital Operational Resilience Act (DORA), which requires financial entities to demonstrate real-time, auditable proof of ICT risk detection and recovery, not just periodic compliance documentation.

Every incident is logged with full context and measured against a Recovery Time Objective (RTO). An MCP server exposes this operational data to Claude, so anyone — engineer or non-technical stakeholder — can ask plain-language questions like *"why did the last incident take 40 seconds to recover?"* and get an answer backed by cited logs and metrics, not a guess.

**Tech stack:** Java 21, Spring Boot 3.x, Angular, Apache Kafka, Resilience4j, Kubernetes (Minikube/Azure AKS), Docker, Grafana, Elasticsearch/Kibana, Azure, Model Context Protocol (MCP), Claude.

**Status:** In development — see [docs/](./docs) for architecture decisions and project plan.
