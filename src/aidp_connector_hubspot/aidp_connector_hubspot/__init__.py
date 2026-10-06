"""HubSpot -> Oracle AI Data Platform ingestion."""

from importlib.metadata import PackageNotFoundError, version

from .auth import HubSpotTokenError, read_token
from .client import HubSpotError
from .config import Config, ConfigError, load_config, parse_config
from .runner import format_summary, raise_on_failure, run

try:
    __version__ = version("aidp-connector-hubspot")
except PackageNotFoundError:  # imported from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Config",
    "ConfigError",
    "HubSpotError",
    "HubSpotTokenError",
    "__version__",
    "format_summary",
    "load_config",
    "parse_config",
    "raise_on_failure",
    "read_token",
    "run",
]
