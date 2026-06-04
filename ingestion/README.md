# Ingestion Scripts

These scripts generate local bronze data for Phase 2. They simulate source-system extracts without using cloud resources.

Run from the repository root:

```powershell
python ingestion/generate_metadata.py
python ingestion/generate_batch_data.py
python ingestion/generate_weather_data.py
```

## Outputs

- `generate_metadata.py`
  - `data_lake/bronze/customer_metadata/customers.csv`
  - `data_lake/bronze/customer_metadata/regions.csv`
  - `data_lake/bronze/meter_metadata/meters.csv`
  - `data_lake/bronze/tariff/tariffs.csv`
- `generate_batch_data.py`
  - Half-hourly consumption CSV files under `data_lake/bronze/historical_consumption/year=YYYY/month=MM/day=DD/`
- `generate_weather_data.py`
  - Hourly weather CSV files under `data_lake/bronze/weather/year=YYYY/month=MM/day=DD/`

The generation scripts preserve `.gitkeep` placeholders and replace generated local files so the project is easy to rerun.
