# Phase 4 Overview

Phase 4 adds a local real-time streaming path to the Azure-Style Energy Lakehouse & Data Warehouse Platform.

Earlier phases created:

- SQL Server warehouse foundation.
- Local batch lakehouse pipeline.
- Airflow orchestration for batch processing.

Phase 4 adds event-driven ingestion and Structured Streaming.

## What Phase 4 Implements

- Python smart-meter event producer.
- Azure Event Hubs Emulator configuration.
- File-mode producer fallback for reliable local execution.
- PySpark Structured Streaming consumers.
- Raw streaming bronze capture.
- Streaming silver clean and rejected event outputs.
- Streaming gold outputs:
  - latest meter readings
  - anomaly events
  - hourly consumption
  - pipeline metrics
- Optional SQL Server load for curated streaming readings and anomalies.
- Event schema, malformed examples, tests, and runbook documentation.

## Why Streaming Is Separate From Batch

Batch data answers historical and periodic analytics questions. Streaming data answers near-real-time operational questions:

- What is the latest reading for each meter?
- Which events are malformed?
- Which meters show anomalous consumption?
- Are events arriving continuously?

The streaming path uses separate folders and checkpoints so it can evolve independently from the batch lakehouse pipeline.

## What Phase 4 Does Not Implement

This phase intentionally does not add:

- FastAPI
- Power BI dashboards
- CI/CD
- Azure cloud deployment

Those are later phases.
