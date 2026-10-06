"""Jira Cloud -> Oracle AI Data Platform ingestion."""

from importlib.metadata import PackageNotFoundError, version

from .auth import read_credentials
from .config import Config, ConfigError, job_parameter, load_config, parse_config
from .errors import JiraAuthError, JiraError, JiraRateLimitError
from .runner import format_summary, preview, run

try:
    __version__ = version("aidp-connector-jira")
except PackageNotFoundError:  # imported from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Config",
    "ConfigError",
    "JiraAuthError",
    "JiraError",
    "JiraRateLimitError",
    "__version__",
    "format_summary",
    "job_parameter",
    "load_config",
    "parse_config",
    "preview",
    "read_credentials",
    "run",
]
