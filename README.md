# MQTT IoT Test Setup

## Description

This project is a temporary test setup for MQTT IoT publish and subscribe architecture.

It is used to simulate message flow between a publisher and a subscriber using MQTT.
This setup is for testing purposes only and **will be removed in the future**.

---

## Project Structure

```
.
├── subscriber     # MQTT subscriber + broker setup (Docker)
├── test-pub       # Test publisher (send simulated payload)
```

---

## Prerequisites

* Node.js
* Docker
* Docker Compose

---

## Setup & Usage

### 1. Start MQTT Broker & Subscriber

Go to the `subscriber` directory:

```bash
cd subscriber
npm install mqtt ioredis
docker-compose build
docker-compose up -d
```

This will:

* Build and start the MQTT broker
* Start the subscriber service
* Listen for incoming MQTT messages

---

### 2. Run Test Publisher

Go to the `test-pub` directory:

```bash
cd test-pub
npm install mqtt ioredis
node test-pub.js
```

This will:

* Connect to the MQTT broker
* Send a simulated **binary payload**

---

## Notes

* This is a **temporary testing environment**
* The architecture is simplified for development/testing

