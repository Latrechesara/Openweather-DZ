---
video_url: "https://www.youtube.com/watch?v=YOUR_VIDEO_ID"
next_url: "02-environment.md"
---

# Introduction: MeteoFlow-DZ & Real-Time Event Streaming Engineering

<img width="1238" height="848" alt="Gemini_Generated_Image_i1r7aqi1r7aqi1r7" src="https://github.com/user-attachments/assets/41129613-894b-4fc4-b871-83cd8963667d" />


Welcome to the **MeteoFlow-DZ Project**! This hands-on, end-to-end real-time data engineering workshop is designed to take you from live external API telemetry streams to a production-ready, interactive geospatial dashboard.

Whether you are building your engineering portfolio or mastering real-time stream processing, this project simulates a modern enterprise event-driven architecture using industry-standard, zero-cost containerized tools.

---

## 🎯 What You Will Learn
By the end of this project, you will know how to:
- **Containerize & Orchestrate:** Orchestrate a multi-container microservice stack (`Kafka`, `ClickHouse`, `Python`, `Dash`) using **Docker Compose**.
- **Modern Message Streaming:** Publish live weather telemetry via **Apache Kafka** operating in modern **KRaft** mode (eliminating legacy ZooKeeper dependencies).
- **High-Throughput Ingestion:** Ingest real-time JSON event streams into **ClickHouse**, leveraging its high-performance columnar `MergeTree` time-series engine.
- **Microservice Ingestion Workers:** Build decoupled, fault-tolerant Python background consumers to handle streaming ingestion without blocking application logic.
- **Visualize Real-Time Data:** Build an interactive geospatial web application using **Plotly Dash** to monitor live temperature, humidity, and wind metrics across key Algerian Wilayas.

Let's dive in and build our real-time streaming pipeline layer by layer!
