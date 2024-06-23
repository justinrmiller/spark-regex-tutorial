"""Test config module."""

import pytest
from unittest.mock import patch, mock_open
from config import get_config, get_spark_job_config, parse_spark_config, get_regex_patterns_config

# Sample YAML content for testing
SPARK_CONFIG_CONTENT = """
spark:
  app1:
    master: "local[*]"
    driver:
      memory: "4g"
      cores: 2
    executor:
      memory: "2g"
      instances: 1
      cores: 1
"""

REGEX_PATTERNS_CONFIG_CONTENT = """
regex_patterns:
  pattern1:
    - '^[a-zA-Z0-9_]+$'
  pattern2:
    - '^\\d{3}-\\d{2}-\\d{4}$'
"""


def test_get_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        get_config()


def test_get_config_success():
    with patch("builtins.open", mock_open(read_data=SPARK_CONFIG_CONTENT)):
        config = get_config("spark-config.yaml")
        assert config["spark"]["app1"]["master"] == "local[*]"


def test_get_spark_job_config():
    app_config = {
        "master": "local[*]",
        "driver": {
            "memory": "4g",
            "cores": 2
        },
        "executor": {
            "memory": "2g",
            "instances": 1,
            "cores": 1
        }
    }
    expected_config = {
        "config": {
            "spark.driver.memory": "4g",
            "spark.driver.cores": 2,
            "spark.executor.memory": "2g",
            "spark.executor.instances": 1,
            "spark.executor.cores": 1
        },
        "master": "local[*]",
        "app": {}
    }
    assert get_spark_job_config(app_config) == expected_config


def test_parse_spark_config():
    with patch("builtins.open", mock_open(read_data=SPARK_CONFIG_CONTENT)):
        with patch("config.SPARK_CONFIG", "spark-config.yaml"):
            config = parse_spark_config()
            assert config["app1"]["config"]["spark.driver.memory"] == "4g"


def test_get_regex_patterns_config():
    with patch("builtins.open", mock_open(read_data=REGEX_PATTERNS_CONFIG_CONTENT)):
        with patch("config.REGEX_PATTERNS_CONFIG", "regex-patterns.yaml"):
            config = get_regex_patterns_config()
            assert config["pattern1"] == ["^[a-zA-Z0-9_]+$"]
