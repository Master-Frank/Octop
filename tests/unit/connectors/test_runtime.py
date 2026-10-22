"""Connector runtime assembly stays outside AgentManager."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from octop.infra.connectors.runtime import ConnectorRuntime


def instance(name: str, kind: str = "open-design") -> SimpleNamespace:
    return SimpleNamespace(instance_id=name, kind=kind, mcp_server_name=name)


@pytest.mark.asyncio
async def test_prepare_only_selected_tools_and_reuses_loaded_tools() -> None:
    service = SimpleNamespace(
        list_user_instances=MagicMock(
            return_value=[instance("selected"), instance("other"), instance("mail", "qq-mail")]
        )
    )
    with patch("octop.infra.connectors.runtime.OpenDesignRuntime") as factory:
        runtime = ConnectorRuntime(service, object())
    factory.return_value.build_tools = AsyncMock(
        return_value=[SimpleNamespace(name="selected_tool")]
    )
    agent = SimpleNamespace(_mcp_tool_name_set=frozenset())

    def append(tools: list[SimpleNamespace]) -> None:
        agent._mcp_tool_name_set = frozenset(t.name for t in tools)

    agent.append_mcp_tools = MagicMock(side_effect=append)
    for _ in range(2):
        await runtime.prepare_tools(agent, agent_id="a", user_id=7, names=["selected", "mail"])
    factory.return_value.build_tools.assert_awaited_once_with(
        user_id=7, instance=service.list_user_instances.return_value[0], agent_id="a"
    )
    service.list_user_instances.assert_called_with(
        7, active_only=True, with_credentials=True, include_shared=True
    )
    agent.append_mcp_tools.assert_called_once()


@pytest.mark.asyncio
async def test_discovery_failure_does_not_block_other_instances(caplog) -> None:
    service = SimpleNamespace(
        list_user_instances=lambda *a, **kw: [instance("bad"), instance("ok")]
    )
    with patch("octop.infra.connectors.runtime.OpenDesignRuntime") as factory:
        runtime = ConnectorRuntime(service, object())
    factory.return_value.build_tools = AsyncMock(side_effect=[RuntimeError("secret"), ["tool"]])
    agent = SimpleNamespace(_mcp_tool_name_set=frozenset(), append_mcp_tools=MagicMock())
    await runtime.prepare_tools(agent, agent_id="a", user_id=7, names=["bad", "ok"])
    agent.append_mcp_tools.assert_called_once_with(["tool"])
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_rebind_and_scoped_close() -> None:
    with patch("octop.infra.connectors.runtime.OpenDesignRuntime") as factory:
        runtime = ConnectorRuntime(object(), object())
    factory.return_value.close = AsyncMock()
    service, repos = object(), object()
    runtime.replace_persistence(service, repos)
    factory.return_value.replace_persistence.assert_called_once_with(service, repos)
    await runtime.close(user_id=7, agent_id="a")
    factory.return_value.close.assert_awaited_once_with(user_id=7, agent_id="a")


@pytest.mark.asyncio
async def test_missing_user_does_not_discover_tools() -> None:
    service = SimpleNamespace(list_user_instances=MagicMock())
    with patch("octop.infra.connectors.runtime.OpenDesignRuntime"):
        runtime = ConnectorRuntime(service, object())
    await runtime.prepare_tools(object(), agent_id="a", user_id=None, names=["selected"])
    service.list_user_instances.assert_not_called()
