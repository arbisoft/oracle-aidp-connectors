"""Notion -> Oracle AI Data Platform ingestion."""

from importlib.metadata import PackageNotFoundError, version

from .auth import NotionTokenError, read_token
from .config import Config, ConfigError, load_config, parse_config
from .runner import NotionAuthError, format_summary, raise_on_failure, run

try:
    __version__ = version("aidp-connector-notion")
except PackageNotFoundError:  # imported from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Config",
    "ConfigError",
    "NotionAuthError",
    "NotionTokenError",
    "__version__",
    "format_summary",
    "load_config",
    "parse_config",
    "raise_on_failure",
    "read_token",
    "run",
]
