"""Find the credentials: the AIDP Credential Store first, then environment variables."""

from __future__ import annotations

import os
from typing import Callable, Mapping, Optional, Tuple

from .client import credentials
from .config import JiraSettings
from .errors import JiraError

# aidputils.secrets.get on AIDP. It is a global the runtime injects into the
# notebook, not an importable module, so the notebook passes it in.
SecretGetter = Callable[..., str]

# Credential Store keys, and the environment variables used outside AIDP.
KEYS = ("site", "email", "token")
ENV = ("JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN")


def read_credentials(
    settings: JiraSettings,
    get_secret: Optional[SecretGetter] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> Tuple[str, str, str]:
    """Return ``(site, email, api_token)``, stripped, with the site checked.

    Reads the keys ``site``, ``email`` and ``token`` of
    ``settings.credential_name`` through ``get_secret`` when both are set.
    Otherwise (no credential named, or not running on AIDP) reads the
    ``JIRA_SITE``, ``JIRA_EMAIL`` and ``JIRA_API_TOKEN`` environment
    variables. Errors name what is missing and never echo a value.
    """
    if settings.credential_name and get_secret is not None:
        return credentials(*(get_secret(name=settings.credential_name, key=key) for key in KEYS))
    environ = os.environ if environ is None else environ
    missing = [name for name in ENV if not str(environ.get(name) or "").strip()]
    if missing:
        raise JiraError(
            "No Jira credentials found. Set jira.credential_name in the config, or export "
            + ", ".join(missing) + "."
        )
    return credentials(*(environ[name] for name in ENV))
