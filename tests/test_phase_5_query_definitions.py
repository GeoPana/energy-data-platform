from pathlib import Path

from api.services.warehouse_queries import MAX_LIMIT, normalize_limit


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def test_serving_views_sql_contains_expected_views() -> None:
    sql_text = read_text("sql/09_create_serving_schema_and_views.sql")
    expected_views = [
        "serving.vw_daily_region_consumption",
        "serving.vw_monthly_region_consumption",
        "serving.vw_customer_usage_summary",
        "serving.vw_meter_latest_reading",
        "serving.vw_anomaly_events",
        "serving.vw_data_quality_summary",
        "serving.vw_pipeline_run_summary",
        "serving.vw_dashboard_kpis",
    ]

    assert "CREATE SCHEMA serving" in sql_text
    for view_name in expected_views:
        assert view_name in sql_text


def test_serving_query_examples_use_serving_schema() -> None:
    sql_text = read_text("sql/10_serving_layer_queries.sql")

    assert "serving.vw_daily_region_consumption" in sql_text
    assert "serving.vw_dashboard_kpis" in sql_text
    assert "serving.vw_pipeline_run_summary" in sql_text
    assert "FROM dw." not in sql_text
    assert "FROM staging." not in sql_text


def test_api_query_definitions_use_parameterized_sql_and_serving_views() -> None:
    query_text = read_text("api/services/warehouse_queries.py")

    assert "serving.vw_daily_region_consumption" in query_text
    assert "serving.vw_meter_latest_reading" in query_text
    assert "serving.vw_anomaly_events" in query_text
    assert "?" in query_text
    assert "staging." not in query_text


def test_limit_normalization_bounds_values() -> None:
    assert normalize_limit(None) == 100
    assert normalize_limit(0) == 1
    assert normalize_limit(MAX_LIMIT + 100) == MAX_LIMIT
