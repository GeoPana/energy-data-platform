# Data Product Contracts

Phase 5 treats each serving view as a small data product.

## `serving.vw_daily_region_consumption`

Purpose: daily energy consumption analytics by region.

Grain: one row per region per date.

Primary consumers: FastAPI, Power BI, SQL analysts.

Key columns: `reading_date`, `region_name`, `total_kwh`, `average_kwh`, `reading_count`, `unique_meter_count`.

Refresh expectation: after batch pipeline completion or warehouse reload.

Source tables: `dw.fact_energy_consumption`, `dw.dim_region`, `dw.dim_date`.

Known limitations: based on loaded warehouse facts, not raw lakehouse files.

## `serving.vw_monthly_region_consumption`

Purpose: monthly regional consumption trends.

Grain: one row per region per month.

Primary consumers: Power BI monthly visuals, FastAPI monthly endpoints.

Key columns: `year_number`, `month_number`, `region_name`, `total_kwh`, `average_daily_kwh`, `unique_meter_count`.

Refresh expectation: after batch pipeline completion.

Source tables: `dw.fact_energy_consumption`, `dw.dim_region`, `dw.dim_date`.

Known limitations: month name is intentionally omitted for stable numeric sorting.

## `serving.vw_customer_usage_summary`

Purpose: customer-level usage profile.

Grain: one row per customer.

Primary consumers: FastAPI customer endpoint, customer drill-through reports.

Key columns: `customer_id`, `customer_segment`, `region_name`, `total_kwh`, `average_daily_kwh`, `first_reading_timestamp`, `last_reading_timestamp`.

Refresh expectation: after batch pipeline completion.

Source tables: `dw.fact_energy_consumption`, `dw.dim_customer`, `dw.dim_region`.

Known limitations: does not expose personally identifiable information.

## `serving.vw_meter_latest_reading`

Purpose: latest smart-meter reading per meter.

Grain: one row per meter.

Primary consumers: FastAPI meter endpoints, smart-meter monitoring dashboard.

Key columns: `meter_id`, `customer_id`, `region_name`, `event_timestamp`, `kwh`, `voltage`.

Refresh expectation: after streaming or batch meter-reading loads.

Source tables: `dw.fact_meter_reading`, `dw.dim_region`.

Known limitations: latest reading depends on records loaded into SQL Server.

## `serving.vw_anomaly_events`

Purpose: anomaly monitoring and investigation.

Grain: one row per anomaly event.

Primary consumers: FastAPI anomaly endpoints, Power BI anomaly table.

Key columns: `anomaly_id`, `meter_id`, `region_name`, `anomaly_type`, `anomaly_score`, `kwh`, `expected_kwh`, `detected_at`.

Refresh expectation: after anomaly detection output is loaded.

Source tables: `dw.fact_anomaly_event`, `dw.dim_region`.

Known limitations: anomaly scoring is intentionally simple in local phases.

## `serving.vw_data_quality_summary`

Purpose: expose data-quality results.

Grain: one row per data-quality check execution.

Primary consumers: monitoring API, operational dashboard.

Key columns: `check_name`, `table_name`, `status`, `failed_count`, `checked_at`.

Refresh expectation: after pipeline quality checks run.

Source tables: `monitoring.data_quality_check`.

Known limitations: depends on pipeline phases writing monitoring data.

## `serving.vw_pipeline_run_summary`

Purpose: expose pipeline run status and counts.

Grain: one row per pipeline run.

Primary consumers: monitoring API, operational dashboard.

Key columns: `pipeline_name`, `pipeline_type`, `status`, `started_at`, `finished_at`, `duration_seconds`, `records_read`, `records_written`, `records_rejected`.

Refresh expectation: as orchestration and pipeline phases record runs.

Source tables: `monitoring.pipeline_run`.

Known limitations: local scripts may not write every operational metric until later hardening.

## `serving.vw_dashboard_kpis`

Purpose: executive KPI summary.

Grain: one row for the current warehouse state.

Primary consumers: dashboard KPI cards, FastAPI dashboard endpoint.

Key columns: `total_kwh`, `total_customers`, `total_meters`, `total_anomalies`, `latest_reading_timestamp`, `failed_pipeline_runs`, `failed_data_quality_checks`.

Refresh expectation: reflects current SQL Server warehouse state.

Source tables: `dw.fact_energy_consumption`, `dw.fact_meter_reading`, `dw.fact_anomaly_event`, `dw.dim_customer`, `dw.dim_meter`, `monitoring.pipeline_run`, `monitoring.data_quality_check`.

Known limitations: single-row KPI surface, not a replacement for detailed views.
