"""Common PySpark DataFrame transforms."""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def add_run_metadata(df: DataFrame, run_date: str, environment: str) -> DataFrame:
    """Append standard run-metadata columns to a DataFrame.

    Adds ``run_date``, ``environment``, and ``processed_at`` (current UTC timestamp).

    Args:
        df: Input DataFrame.
        run_date: Logical run date string (yyyy-MM-dd).
        environment: Target environment name (e.g. ``staging``, ``prod``).
    """
    return (
        df.withColumn("run_date", F.lit(run_date))
        .withColumn("environment", F.lit(environment))
        .withColumn("processed_at", F.current_timestamp())
    )


def deduplicate_by_key(df: DataFrame, key_columns: list, order_column: str) -> DataFrame:
    """Keep only the latest row per key based on an ordering column.

    Args:
        df: Input DataFrame.
        key_columns: Column names that form the dedup key.
        order_column: Column used for ordering (descending); the first row wins.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
