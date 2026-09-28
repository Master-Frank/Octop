"""Unit tests for bridge agent id helpers."""

from __future__ import annotations

import pytest

from octop.infra.bridge.ids import (
    format_bridge_agent_id,
    is_bridge_agent_id,
    parse_bridge_agent_id,
)


def test_format_and_parse_roundtrip() -> None:
    agent_id = format_bridge_agent_id("01ABCDEF", "agent-1")
    assert agent_id == "bridge:01ABCDEF:agent-1"
    ref = parse_bridge_agent_id(agent_id)
    assert ref is not None
    assert ref.connection_id == "01ABCDEF"
    assert ref.remote_agent_id == "agent-1"
    assert is_bridge_agent_id(agent_id)


def test_parse_rejects_local_ids() -> None:
    assert parse_bridge_agent_id("01LOCALAGENT") is None
    assert not is_bridge_agent_id("01LOCALAGENT")


def test_format_rejects_colon_in_connection_id() -> None:
    with pytest.raises(ValueError):
        format_bridge_agent_id("bad:id", "agent")
