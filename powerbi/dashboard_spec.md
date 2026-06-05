# Power BI Dashboard Specification

## Page 1: Executive Overview

Purpose: summarize the platform and energy usage at a glance.

Visuals:

- KPI card: Total kWh
- KPI card: Total customers
- KPI card: Total meters
- KPI card: Total anomalies
- KPI card: Latest reading timestamp
- Line chart: daily consumption by region
- Bar chart: monthly consumption by region

Serving views:

- `serving.vw_dashboard_kpis`
- `serving.vw_daily_region_consumption`
- `serving.vw_monthly_region_consumption`

## Page 2: Regional Consumption

Purpose: analyze usage trends by London region.

Visuals:

- Line chart: daily kWh by region
- Matrix: region by month
- Slicer: region
- Slicer: date range

Serving views:

- `serving.vw_daily_region_consumption`
- `serving.vw_monthly_region_consumption`

## Page 3: Smart Meter Monitoring

Purpose: monitor latest meter readings and anomalies.

Visuals:

- Table: latest reading per meter
- Bar chart: top meters by latest kWh
- Bar chart: anomaly count by region
- Table: anomaly details

Serving views:

- `serving.vw_meter_latest_reading`
- `serving.vw_anomaly_events`

## Page 4: Data Quality And Pipeline Monitoring

Purpose: expose operational health to data consumers.

Visuals:

- Table: pipeline run status
- Table: failed data-quality checks
- KPI card: failed pipeline runs
- KPI card: failed data-quality checks
- KPI card: data freshness

Serving views:

- `serving.vw_pipeline_run_summary`
- `serving.vw_data_quality_summary`
- `serving.vw_dashboard_kpis`

## Design Notes

- Keep visuals clean and business-facing.
- Use serving views only.
- Use slicers sparingly: region, date range, status.
- Add a small text box naming the data source: SQL Server Developer, `EnergyWarehouse`, `serving` schema.
