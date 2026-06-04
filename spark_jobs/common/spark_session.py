"""Spark session factory for local Phase 2 jobs."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Create a local Spark session tuned for a small portfolio project.

    The project writes silver and gold data as Parquet for reliable local
    execution. Delta Lake can be enabled later in a Databricks or delta-spark
    environment without changing the medallion model.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
