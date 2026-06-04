# Airflow To Azure Data Factory Mapping

Phase 3 uses Airflow locally, but the orchestration concepts map directly to Azure Data Factory.

| Local Airflow Concept | Azure Data Factory Concept |
| --- | --- |
| Airflow DAG | ADF pipeline |
| Airflow task | ADF activity |
| Airflow schedule | ADF trigger |
| Airflow task dependency | ADF activity dependency |
| Airflow retry settings | ADF retry policy |
| Airflow logs | ADF Monitor logs |
| Airflow Grid/Graph view | ADF Monitor pipeline run view |
| Airflow Variables and Connections | ADF parameters, linked services, and Key Vault references |
| Airflow XCom | ADF activity output passed to later activities |
| Airflow task failure callback | ADF failure path or alerting action |

## Why This Matters

The portfolio project stays free and local, but the design demonstrates cloud orchestration thinking:

- pipelines have clear dependencies
- tasks are observable
- retries are explicit
- quality gates run after loading
- credentials are externalized
- monitoring is captured in a warehouse table

In a future Azure version, the same pipeline could be represented as an ADF pipeline that triggers Databricks notebooks, Synapse Spark jobs, SQL stored procedures, and quality checks.
