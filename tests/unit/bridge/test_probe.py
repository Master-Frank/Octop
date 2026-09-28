"""Unit tests for Bridge peer probe summaries."""

from __future__ import annotations

from octop.infra.bridge.manager import _absolute_peer_url, _probe_agent_summary


def test_absolute_peer_url_joins_relative_icon() -> None:
    assert (
        _absolute_peer_url("https://demo.octop.chat", "/api/agents/abc/icon")
        == "https://demo.octop.chat/api/agents/abc/icon"
    )


def test_absolute_peer_url_keeps_absolute() -> None:
    url = "https://cdn.example/icon.png"
    assert _absolute_peer_url("https://demo.octop.chat", url) == url


def test_absolute_peer_url_empty() -> None:
    assert _absolute_peer_url("https://demo.octop.chat", None) is None
    assert _absolute_peer_url("https://demo.octop.chat", "  ") is None


def test_probe_agent_summary_maps_fields() -> None:
    summary = _probe_agent_summary(
        {
            "agent_id": "01AGENT",
            "id": 42,
            "name": "Demo Expert",
            "description": "Helps with demos",
            "icon_url": "/api/agents/01AGENT/icon",
            "color": "#3366ff",
            "state": "running",
            "kind": "expert",
        },
        peer_base_url="https://demo.octop.chat",
    )
    assert summary == {
        "agent_id": "01AGENT",
        "name": "Demo Expert",
        "description": "Helps with demos",
        "icon_url": "https://demo.octop.chat/api/agents/01AGENT/icon",
        "icon_name": None,
        "color": "#3366ff",
        "state": "running",
        "kind": "expert",
    }


def test_probe_agent_summary_prefers_agent_id_over_int_id() -> None:
    summary = _probe_agent_summary(
        {"id": 7, "agent_id": "public-id", "name": "X"},
        peer_base_url="https://peer.example",
    )
    assert summary["agent_id"] == "public-id"
    assert summary["description"] is None
    assert summary["icon_url"] is None
