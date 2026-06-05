# Power BI Dashboard Design

Phase 5 defines a Power BI Desktop dashboard over the SQL Server serving views.

## Dashboard Pages

1. Executive Overview
2. Regional Consumption
3. Smart Meter Monitoring
4. Data Quality And Pipeline Monitoring

## Recommended Visuals

Executive Overview:

- KPI cards from `serving.vw_dashboard_kpis`
- daily region trend from `serving.vw_daily_region_consumption`
- monthly region trend from `serving.vw_monthly_region_consumption`

Regional Consumption:

- line chart by `reading_date` and `region_name`
- matrix by region and month
- region and date slicers

Smart Meter Monitoring:

- latest meter reading table
- anomaly count by region
- anomaly detail table

Data Quality And Pipeline Monitoring:

- pipeline run status table
- failed quality checks table
- freshness and failure KPI cards

## Refresh Strategy

Use Import mode locally and refresh manually after:

- Phase 1 seed changes
- Phase 2 batch pipeline runs
- Phase 3 Airflow DAG runs
- Phase 4 streaming SQL load runs

## Screenshots Workflow

After building the report in Power BI Desktop:

1. Capture each dashboard page as an image.
2. Save images to `powerbi/screenshots/`.
3. Reference the screenshots in the project README or portfolio write-up.

Do not commit private data or credentials.
