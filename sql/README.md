# SQL Script Guide

Run the SQL files in numeric order. The scripts use SQL Server / T-SQL syntax and are intended for SQL Server Management Studio, Azure Data Studio, the VS Code MSSQL extension, or `sqlcmd`.

| Order | Script | Purpose |
| --- | --- | --- |
| 1 | `01_create_database.sql` | Creates the `EnergyWarehouse` database if it does not already exist. |
| 2 | `02_create_schemas.sql` | Creates `staging`, `dw`, and `monitoring` schemas plus staging and monitoring tables. |
| 3 | `03_create_dimensions.sql` | Creates warehouse dimension tables and supporting dimension indexes. |
| 4 | `04_create_facts.sql` | Creates warehouse fact tables, foreign keys, checks, and useful fact indexes. |
| 5 | `05_seed_sample_data.sql` | Inserts coherent London-style sample data for local development and query demos. |
| 6 | `06_data_quality_checks.sql` | Provides data-quality investigation queries for common warehouse issues. |
| 7 | `07_analytics_queries.sql` | Provides analytics examples using joins, CTEs, aggregations, and window functions. |
| 8 | `08_drop_all.sql` | Drops all Phase 1 tables and schemas in dependency order. |

## Recommended Execution

```powershell
sqlcmd -S localhost -E -i sql\01_create_database.sql
sqlcmd -S localhost -E -i sql\02_create_schemas.sql
sqlcmd -S localhost -E -i sql\03_create_dimensions.sql
sqlcmd -S localhost -E -i sql\04_create_facts.sql
sqlcmd -S localhost -E -i sql\05_seed_sample_data.sql
```

Run the quality and analytics scripts after the seed script:

```powershell
sqlcmd -S localhost -E -i sql\06_data_quality_checks.sql
sqlcmd -S localhost -E -i sql\07_analytics_queries.sql
```

## Notes

- `GO` batch separators are included, so use a SQL Server-aware tool.
- Setup scripts are rerunnable where reasonable.
- The seed script avoids duplicating its sample rows when rerun.
- `08_drop_all.sql` is destructive for Phase 1 objects and should be used only when you want a clean rebuild.
- Later phases can add PySpark, Airflow, streaming, API, reporting, and CI/CD assets without changing the Phase 1 run order.
