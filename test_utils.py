"""Test Utils."""

import pytest
from unittest.mock import patch, mock_open
from config import get_regex_patterns_config

# Sample YAML content for testing
REGEX_PATTERNS_CONFIG_CONTENT = """
regex_patterns:
  pattern1:
    - '^[a-zA-Z0-9_]+$'
  pattern2:
    - '^\\d{3}-\\d{2}-\\d{4}$'
"""


def test_get_regex_patterns_config():
    with patch("builtins.open", mock_open(read_data=REGEX_PATTERNS_CONFIG_CONTENT)):
        with patch("config.REGEX_PATTERNS_CONFIG", "regex-patterns.yaml"):
            regex_patterns = get_regex_patterns_config()
            assert regex_patterns["pattern1"] == ["^[a-zA-Z0-9_]+$"]
            assert regex_patterns["pattern2"] == ["^\\d{3}-\\d{2}-\\d{4}$"]


if __name__ == "__main__":
    pytest.main()
