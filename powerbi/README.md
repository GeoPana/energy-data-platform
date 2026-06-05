# Power BI Desktop Consumption Layer

Phase 5 uses Power BI Desktop as the free local reporting tool.

The dashboard should connect to SQL Server Developer and use the `serving` schema views, not raw warehouse tables.

## Prerequisites

- Power BI Desktop installed locally.
- SQL Server Developer running.
- `EnergyWarehouse` database created.
- Phase 5 serving views created:

```text
sql/09_create_serving_schema_and_views.sql
```

## Connect Power BI Desktop

1. Open Power BI Desktop.
2. Select **Get Data**.
3. Choose **SQL Server**.
4. Server: `localhost`
5. Database: `EnergyWarehouse`
6. Choose Import mode for a simple local portfolio dashboard.
7. Select views from the `serving` schema:
   - `serving.vw_daily_region_consumption`
   - `serving.vw_monthly_region_consumption`
   - `serving.vw_customer_usage_summary`
   - `serving.vw_meter_latest_reading`
   - `serving.vw_anomaly_events`
   - `serving.vw_data_quality_summary`
   - `serving.vw_pipeline_run_summary`
   - `serving.vw_dashboard_kpis`

## Import vs DirectQuery

Use Import mode for the portfolio MVP. It is simpler, faster locally, and easy to refresh manually.

DirectQuery is closer to some production patterns, but it adds complexity that is unnecessary for this phase.

## Refresh Expectations

Refresh Power BI after:

- Phase 1 seed data changes.
- Phase 2 batch pipeline loads data into SQL Server.
- Phase 3 Airflow DAG completes.
- Phase 4 streaming SQL load runs.

## Screenshots For GitHub

Save dashboard screenshots under:

```text
powerbi/screenshots/
```

Recommended screenshots:

- Executive overview page.
- Regional consumption page.
- Smart-meter monitoring page.
- Data-quality and pipeline monitoring page.

Do not commit private data or credentials.
