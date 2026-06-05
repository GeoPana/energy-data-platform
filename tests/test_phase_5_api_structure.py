from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def test_phase_5_api_files_exist() -> None:
    expected_files = [
        "api/main.py",
        "api/db.py",
        "api/config.py",
        "api/models.py",
        "api/routers/__init__.py",
        "api/routers/health.py",
        "api/routers/regions.py",
        "api/routers/meters.py",
        "api/routers/anomalies.py",
        "api/routers/consumption.py",
        "api/routers/monitoring.py",
        "api/services/__init__.py",
        "api/services/warehouse_queries.py",
        "api/README.md",
    ]

    for relative_path in expected_files:
        assert (PROJECT_ROOT / relative_path).exists()


def test_phase_5_expected_endpoint_strings_exist() -> None:
    combined_api_text = "\n".join(
        [
            read_text("api/routers/health.py"),
            read_text("api/routers/regions.py"),
            read_text("api/routers/meters.py"),
            read_text("api/routers/anomalies.py"),
            read_text("api/routers/consumption.py"),
            read_text("api/routers/monitoring.py"),
        ]
    )

    expected_fragments = [
        '"/health"',
        '"/version"',
        '"/{region_name}/daily-consumption"',
        '"/{region_name}/monthly-consumption"',
        '"/{meter_id}/latest-reading"',
        '"/latest-readings"',
        '"/region/{region_name}"',
        '"/consumption/daily"',
        '"/consumption/monthly"',
        '"/consumption/customer/{customer_id}"',
        '"/dashboard/kpis"',
        '"/pipeline-runs"',
        '"/data-quality"',
        '"/freshness"',
    ]

    for fragment in expected_fragments:
        assert fragment in combined_api_text


def test_phase_5_docs_and_powerbi_files_exist() -> None:
    expected_files = [
        "powerbi/README.md",
        "powerbi/dashboard_spec.md",
        "powerbi/measures.md",
        "powerbi/screenshots/.gitkeep",
        "docs/phase_5_overview.md",
        "docs/serving_layer_design.md",
        "docs/api_design.md",
        "docs/powerbi_dashboard_design.md",
        "docs/data_product_contracts.md",
    ]

    for relative_path in expected_files:
        assert (PROJECT_ROOT / relative_path).exists()
