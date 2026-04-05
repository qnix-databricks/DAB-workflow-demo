"""Configuration helpers for Databricks workflows."""

import json
from pyspark.sql import SparkSession


def load_config(spark: SparkSession, config_path: str) -> dict:
    """Read a JSON config file from a Unity Catalog Volume and return it as a dict.

    Args:
        spark: Active SparkSession.
        config_path: Absolute path to the JSON file on a UC Volume,
                     e.g. ``/Volumes/users/quan_ta/config/app_conf.json``.
    """
    df = spark.read.text(config_path)
    content = "".join(row.value for row in df.collect())
    return json.loads(content)


def get_volume_config(spark: SparkSession, config_path: str, key: str, default=None):
    """Return a single value from a JSON config stored on a Volume.

    Args:
        spark: Active SparkSession.
        config_path: Path to the JSON config file.
        key: Top-level key to retrieve.
        default: Fallback if the key is missing.
    """
    cfg = load_config(spark, config_path)
    return cfg.get(key, default)
