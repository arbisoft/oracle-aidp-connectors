"""MongoDB -> Oracle AI Data Platform ingestion."""

from importlib.metadata import PackageNotFoundError, version

from .auth import read_uri
from .config import Config, ConfigError, job_parameter, load_config, parse_config
from .reader import MongoAuthError, MongoError
from .runner import format_summary, run

try:
    __version__ = version("aidp-connector-mongodb")
except PackageNotFoundError:  # imported from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Config",
    "ConfigError",
    "MongoAuthError",
    "MongoError",
    "__version__",
    "format_summary",
    "job_parameter",
    "load_config",
    "parse_config",
    "read_uri",
    "run",
]
