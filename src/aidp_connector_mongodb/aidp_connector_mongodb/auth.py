"""Find the connection URI: the AIDP Credential Store first, then an environment variable."""

from __future__ import annotations

import os
from typing import Callable, Mapping, Optional

from .config import MongoSettings
from .reader import MongoError, validate_uri

# aidputils.secrets.get on AIDP. It is a global the runtime injects into the
# notebook, not an importable module, so the notebook passes it in.
SecretGetter = Callable[..., str]


def read_uri(
    settings: MongoSettings,
    get_secret: Optional[SecretGetter] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Return the connection URI named by ``settings``, validated and stripped.

    Reads ``settings.credential_name`` through ``get_secret`` when both are
    set. Otherwise (no credential named, or not running on AIDP) falls back to
    the ``settings.uri_env`` environment variable. Errors never echo the URI.
    """
    if settings.credential_name and get_secret is not None:
        uri = get_secret(name=settings.credential_name, key=settings.credential_key)
        if not uri:
            raise MongoError(
                f"Credential '{settings.credential_name}' has no value for key '{settings.credential_key}'."
            )
        return validate_uri(uri)
    uri = (os.environ if environ is None else environ).get(settings.uri_env)
    if not uri:
        raise MongoError(
            f"No connection URI found. Set mongodb.credential_name in the config, or export {settings.uri_env}."
        )
    return validate_uri(uri)
