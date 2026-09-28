"""Bridge remote agent id helpers: ``bridge:{connection_id}:{remote_agent_id}``."""

from __future__ import annotations

from dataclasses import dataclass

BRIDGE_AGENT_PREFIX = "bridge:"
_BRIDGE_PROTOCOL_VERSION = 1

PROTOCOL_VERSION = _BRIDGE_PROTOCOL_VERSION


@dataclass(frozen=True)
class BridgeAgentRef:
    connection_id: str
    remote_agent_id: str

    @property
    def agent_id(self) -> str:
        return format_bridge_agent_id(self.connection_id, self.remote_agent_id)


def format_bridge_agent_id(connection_id: str, remote_agent_id: str) -> str:
    cid = connection_id.strip()
    aid = remote_agent_id.strip()
    if not cid or not aid:
        raise ValueError("connection_id and remote_agent_id required")
    if ":" in cid:
        raise ValueError("connection_id must not contain ':'")
    return f"{BRIDGE_AGENT_PREFIX}{cid}:{aid}"


def parse_bridge_agent_id(agent_id: str) -> BridgeAgentRef | None:
    raw = (agent_id or "").strip()
    if not raw.startswith(BRIDGE_AGENT_PREFIX):
        return None
    rest = raw[len(BRIDGE_AGENT_PREFIX) :]
    connection_id, sep, remote_agent_id = rest.partition(":")
    if not sep or not connection_id.strip() or not remote_agent_id.strip():
        return None
    return BridgeAgentRef(
        connection_id=connection_id.strip(),
        remote_agent_id=remote_agent_id.strip(),
    )


def is_bridge_agent_id(agent_id: str) -> bool:
    return parse_bridge_agent_id(agent_id) is not None
