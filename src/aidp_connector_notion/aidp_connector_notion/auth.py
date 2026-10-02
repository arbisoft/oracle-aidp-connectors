"""Find the Notion token: the AIDP Credential Store first, then an environment variable."""

from __future__ import annotations

import os
from typing import Callable, Mapping, Optional

from .config import NotionSettings

# aidputils.secrets.get on AIDP. It is a global the runtime injects into the
# notebook, not an importable module, so the notebook passes it in.
SecretGetter = Callable[..., str]


class NotionTokenError(RuntimeError):
    """No Notion token could be found."""


def read_token(
    settings: NotionSettings,
    get_secret: Optional[SecretGetter] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Return the Notion token named by ``settings``.

    Reads ``settings.credential_name`` through ``get_secret`` when both are
    set. Otherwise (no credential named, or not running on AIDP) falls back to
    the ``settings.token_env`` environment variable.
    """
    if settings.credential_name and get_secret is not None:
        token = get_secret(name=settings.credential_name, key=settings.credential_key)
        if not token:
            raise NotionTokenError(
                f"Credential '{settings.credential_name}' has no value for key '{settings.credential_key}'."
            )
        return token
    token = (os.environ if environ is None else environ).get(settings.token_env)
    if not token:
        raise NotionTokenError(
            f"No Notion token found. Set notion.credential_name in the config, "
            f"or export {settings.token_env}."
        )
    return token
