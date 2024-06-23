"""Config"""

from typing import Dict
from yaml import safe_load
import json

SPARK_CONFIG = "spark-config.yaml"
REGEX_PATTERNS_CONFIG = "regex-patterns.yaml"


def get_config(config_file=None):
    """Get config file from YAML"""
    if config_file is None:
        raise FileNotFoundError("Config file not found")
    try:
        yaml_config = safe_load(open(config_file))
        return yaml_config
    except FileNotFoundError:
        raise


def get_spark_job_config(app_config) -> Dict:
    """Get spark job config from YAML"""
    spark_app_config = dict()
    master = app_config.get("master", "local[*]")
    driver = app_config.get("driver", {})
    executor = app_config.get("executor", {})
    spark_app_config["config"] = {}
    for key in driver.keys():
        spark_app_config["config"][f"spark.driver.{key}"] = driver[key]
    for key in executor.keys():
        spark_app_config["config"][f"spark.executor.{key}"] = executor[key]
    spark_app_config["master"] = master
    spark_app_config["app"] = app_config.get("app", {})
    return spark_app_config


def parse_spark_config() -> Dict:
    """Parse Spark config"""
    yaml_config = get_config(SPARK_CONFIG)
    spark_config = dict()
    spark = yaml_config.get("spark", {})
    for key in spark.keys():
        spark_config[key] = get_spark_job_config(spark[key])
    return spark_config


def get_regex_patterns_config() -> Dict:
    """Get regex patterns config"""
    yaml_config = get_config(REGEX_PATTERNS_CONFIG)
    regex_patterns_config = dict()
    regex_patterns = yaml_config.get("regex_patterns", {})
    for key in regex_patterns.keys():
        regex_patterns_config[key] = regex_patterns[key]
    return regex_patterns_config


if __name__ == "__main__":
    config = parse_spark_config()
    print(json.dumps(config, indent=2))

    regex_config = get_regex_patterns_config()
    print(json.dumps(regex_config, indent=2))
    """
    Generates JSON dict
    
    {
      "config": {
        "spark.driver.memory": "12g",
        "spark.driver.cores": 1,
        "spark.executor.memory": "4g",
        "spark.executor.instances": 4,
        "spark.executor.cores": 1
      },
      "master": "spark://processing:7077",
      "app": "Scan"
    }
    """
