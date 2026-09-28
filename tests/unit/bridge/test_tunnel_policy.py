"""Unit tests for Bridge HTTP tunnel path allowlist."""

from __future__ import annotations

from octop.infra.bridge.tunnel_policy import is_tunnel_path_allowed


def test_allows_agent_list_get() -> None:
    assert is_tunnel_path_allowed("GET", "/api/agents")
    assert is_tunnel_path_allowed("GET", "/api/agents/")
    assert not is_tunnel_path_allowed("POST", "/api/agents")


def test_allows_agent_resource_paths() -> None:
    assert is_tunnel_path_allowed("GET", "/api/agents/01ABC")
    assert is_tunnel_path_allowed("GET", "/api/agents/01ABC/threads")
    assert is_tunnel_path_allowed("POST", "/api/agents/01ABC/uploads")
    assert is_tunnel_path_allowed("GET", "/api/agents/bridge:cid:aid/avatar")
    assert is_tunnel_path_allowed("GET", "/api/agents/01ABC/history/versions")


def test_allows_composer_readonly_paths() -> None:
    assert is_tunnel_path_allowed("GET", "/api/providers/resolved")
    assert is_tunnel_path_allowed("GET", "/api/providers/active-model")
    assert is_tunnel_path_allowed("GET", "/api/knowledge-bases")
    assert is_tunnel_path_allowed("GET", "/api/knowledge-bases/capability")
    assert is_tunnel_path_allowed("GET", "/api/agents/01ABC/subagents")
    assert not is_tunnel_path_allowed("POST", "/api/providers/resolved")
    assert not is_tunnel_path_allowed("PUT", "/api/providers/active-model")
    assert not is_tunnel_path_allowed("POST", "/api/knowledge-bases")
    assert not is_tunnel_path_allowed("GET", "/api/knowledge-bases/kb1")
    assert not is_tunnel_path_allowed("GET", "/api/knowledge-bases/kb1/documents")
    assert not is_tunnel_path_allowed("GET", "/api/providers")


def test_denies_management_and_auth_paths() -> None:
    assert not is_tunnel_path_allowed("GET", "/api/users")
    assert not is_tunnel_path_allowed("POST", "/api/auth/login")
    assert not is_tunnel_path_allowed("GET", "/api/bridge/connections")
    assert not is_tunnel_path_allowed("GET", "/api/admin/audit")
    assert not is_tunnel_path_allowed("POST", "/api/settings")
    assert not is_tunnel_path_allowed("DELETE", "/api/setup/password")
