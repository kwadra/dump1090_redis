import pytest

from config import redact_url_password, Config


def test_redacts_password_in_url_with_password():
    url = "redis://user:password@localhost:6379/0"
    redacted_url = redact_url_password(url)
    assert redacted_url == "redis://user:<redacted>@localhost:6379/0"

def test_returns_original_url_if_no_password():
    url = "redis://localhost:6379/0"
    redacted_url = redact_url_password(url)
    assert redacted_url == url

def test_handles_invalid_url_format():
    url = "invalid_url"
    redacted_url = redact_url_password(url)
    assert redacted_url == url

def test_returns_none_if_url_is_none():
    url = None
    redacted_url = redact_url_password(url)
    assert redacted_url is None

def test_config_object_allows_attribute_access():
    config = Config({"TEST_KEY": "value"})
    assert config.TEST_KEY == "value"
    assert config.test_key == "value"

def test_config_object_allows_dict_access():
    config = Config({"TEST_KEY": "value"})
    assert config["TEST_KEY"] == "value"
    assert config["test_key"] == "value"

def test_config_object_allows_attribute_modification():
    config = Config({"TEST_KEY": "value"})
    config.TEST_KEY = "new_value"
    assert config.TEST_KEY == "new_value"
    #assert config.test_key is None

def test_config_object_allows_dict_modification():
    config = Config({"TEST_KEY": "value"})
    config["TEST_KEY"] = "new_value"
    assert config.TEST_KEY == "new_value"
    #assert config.test_key == "new_value"