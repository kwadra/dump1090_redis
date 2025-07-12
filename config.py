# define all the configuration variables for the application
# by grabbing from os.environ
import os
import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()

config_values = {
    "REDIS_URL": os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    "FA_HOST": os.environ.get("FA_HOST", "localhost"),
    "MQTT_HOST": os.environ.get("MQTT_HOST", "localhost"),
    "MQTT_TOPIC_NAME": os.environ.get("MQTT_TOPIC_NAME", "flightaware/positions"),
    "MQTT_DISTANCE_MAX": float(os.environ.get("MQTT_DISTANCE_MAX", "1.0")),
    "HOME_LATITUDE": float(os.environ.get("HOME_LATITUDE", "0.0")),
    "HOME_LONGITUDE": float(os.environ.get("HOME_LONGITUDE", "0.0")),
    "LOG_FILENAME": os.environ.get("LOG_FILENAME", "/var/log/adsb-flightaware-redis.log"),
}

def redact_url_password(url):
    """Redact the password in a URL."""
    if url is None:
        return None
    parts = url.split("://")
    if len(parts) != 2:
        return url  # Not a valid URL format
    scheme, rest = parts
    if "@" in rest:
        user_pass, host_port = rest.split("@", 1)
        user, _ = user_pass.split(":", 1) if ":" in user_pass else (user_pass, "")
        return f"{scheme}://{user}:<redacted>@{host_port}"
    return url  # No password to redact

# create object where we can store the configuration
class Config:
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)
            # set lower case version of the key as well
            setattr(self, key.lower(), value)
    # make accessable as a dictionary
    def __getitem__(self, key):
        return getattr(self, key)
    def __setitem__(self, key, value):
        setattr(self, key, value)



CONFIG = Config(config_values)