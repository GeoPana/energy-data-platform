# Suggested Power BI Measures

Use these simple DAX measures as a starting point.

## Total kWh

```DAX
Total kWh = SUM('vw_daily_region_consumption'[total_kwh])
```

## Average Daily kWh

```DAX
Average Daily kWh = AVERAGE('vw_daily_region_consumption'[total_kwh])
```

## Total Anomalies

```DAX
Total Anomalies = COUNTROWS('vw_anomaly_events')
```

## Active Meters

```DAX
Active Meters = DISTINCTCOUNT('vw_meter_latest_reading'[meter_id])
```

## Latest Reading Timestamp

```DAX
Latest Reading Timestamp = MAX('vw_dashboard_kpis'[latest_reading_timestamp])
```

## Failed Pipeline Runs

```DAX
Failed Pipeline Runs =
CALCULATE(
    COUNTROWS('vw_pipeline_run_summary'),
    'vw_pipeline_run_summary'[status] = "failed"
)
```

## Failed Data Quality Checks

```DAX
Failed Data Quality Checks =
CALCULATE(
    COUNTROWS('vw_data_quality_summary'),
    'vw_data_quality_summary'[status] = "failed"
)
```

## Anomaly Rate

```DAX
Anomaly Rate =
DIVIDE(
    [Total Anomalies],
    [Active Meters],
    0
)
```
