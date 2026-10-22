"""Host lifecycle and tool assembly for managed connector runtimes."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from octop.infra.connectors.open_design import OpenDesignRuntime

logger = logging.getLogger(__name__)


class ConnectorRuntime:
    """Keep connector-specific assembly outside the agent orchestrator."""

    def __init__(self, service: Any, repos: Any) -> None:
        self._service = service
        self.open_design = OpenDesignRuntime(service, repos)

    def replace_persistence(self, service: Any, repos: Any) -> None:
        self._service = service
        self.open_design.replace_persistence(service, repos)

    async def close(self, *, user_id: int | None = None, agent_id: str | None = None) -> None:
        await self.open_design.close(user_id=user_id, agent_id=agent_id)

    async def prepare_tools(
        self, agent: Any, *, agent_id: str, user_id: int | None, names: list[str]
    ) -> None:
        """Attach selected managed connector tools once per agent runtime."""
        if user_id is None:
            return
        design_instances = await asyncio.to_thread(
            self._service.list_user_instances,
            user_id,
            active_only=True,
            with_credentials=True,
            include_shared=True,
        )
        for instance in design_instances:
            if instance.kind != "open-design" or instance.mcp_server_name not in names:
                continue
            prefix = f"{instance.mcp_server_name}_"
            if any(t.startswith(prefix) for t in getattr(agent, "_mcp_tool_name_set", frozenset())):
                continue
            try:
                tools = await self.open_design.build_tools(
                    user_id=user_id, instance=instance, agent_id=agent_id
                )
                agent.append_mcp_tools(tools)
            except (Exception, BaseExceptionGroup):
                logger.warning(
                    "OpenDesign tool discovery failed for instance %s", instance.instance_id
                )
