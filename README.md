# Real-Time Air Quality (AQI) Tracker

A distributed microservices system for real-time global Air Quality Index (AQI) monitoring, featuring anomaly detection based on the Actor Model.

## System Architecture

The project is split into two main microservices communicating asynchronously via **Apache Kafka**, ensuring scalability and resilience:

1. **Poller Service (Go):** A concurrent worker-pool that periodically queries the WAQI (World Air Quality Index) API for dozens of cities. It parses JSON responses, handles rate-limiting via jittering, and publishes raw payloads to Kafka.
2. **Monitor Service (Scala/Akka):** Consumes the Kafka stream and routes the data to a dedicated Akka actor for each city (`CityActor`). These actors maintain an in-memory state, calculate the Exponential Moving Average (EMA), and trigger alerts for sudden spikes or critical thresholds.

## Tech Stack

* **Backend Poller:** Go 1.24
* **Backend Monitor:** Scala 2.13.12 & Akka 2.8.5 (Actor Model, Akka HTTP, Akka Streams)
* **Message Broker:** Apache Kafka & Zookeeper
* **Database:** PostgreSQL 15 (managed via Slick 3.5.0)
* **Infrastructure:** Docker & Docker Compose (Alpine and Temurin-based images)

## Running Locally

### Prerequisites
* Docker and Docker Compose installed.
* A free API key from [WAQI](https://aqicn.org/data-platform/token/).

### Quickstart
1. Clone the repository.
2. Rename the `.env.example` file to `.env` and insert your API key:
   ```env
   AQI_API_KEY=your_token_here
   DB_USER=postgres
   DB_PASS=secretpassword
   DB_NAME=aqi_tracker
   ```
3. Boot up the infrastructure using Docker Compose:
   ```docker-compose up -d```