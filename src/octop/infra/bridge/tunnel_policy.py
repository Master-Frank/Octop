"""Path policy for Bridge HTTP tunnel requests against the local ASGI app."""

from __future__ import annotations

import re

# Only agent-scoped product surfaces may be executed on behalf of the connection
# owner. Management / auth / bridge control planes stay local-only.
_AGENT_COLLECTION = re.compile(r"^/api/agents/?$")
_AGENT_RESOURCE = re.compile(
    r"^/api/agents/"
    r"(?:bridge:[^/]+|[^/]+)"
    r"(?:/(?:threads|history|uploads|avatar|icon|chat|messages|files|workspace"
    r"|attachments|media|turns|memory|skills|tools|mbti|persona|channels"
    r"|cron|config|state|welcome|members|subagents)(?:/.*)?)?$"
)

# Composer read-only surfaces for remote chat (models + knowledge pickers).
# Write / document / admin provider routes stay denied.
_COMPOSER_READONLY = re.compile(
    r"^/api/(?:"
    r"providers/resolved|"
    r"providers/active-model|"
    r"knowledge-bases|"
    r"knowledge-bases/capability"
    r")$"
)


def is_tunnel_path_allowed(method: str, path: str) -> bool:
    """Return True when ``method`` + ``path`` may run via an inbound tunnel."""
    verb = (method or "GET").upper()
    raw = (path or "").split("?", 1)[0].strip() or "/"
    if not raw.startswith("/"):
        raw = f"/{raw}"
    # Normalize trailing slash except root.
    if len(raw) > 1:
        raw = raw.rstrip("/")

    if _AGENT_COLLECTION.fullmatch(raw):
        return verb == "GET"

    if _COMPOSER_READONLY.fullmatch(raw):
        return verb == "GET"

    if _AGENT_RESOURCE.fullmatch(raw):
        return verb in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}

    return False
