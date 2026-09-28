"""Unit tests for BridgeManager composer tunnel helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from octop.infra.bridge.manager import BridgeManager
from octop.infra.errors import ErrorCode, OctopError


def _mgr() -> BridgeManager:
    return BridgeManager(
        bridge_repo=MagicMock(),
        secret_repo=MagicMock(),
        user_repo=MagicMock(),
        advertise_base_url="http://local.test",
    )


def _json_response(payload: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


@pytest.mark.asyncio
async def test_list_remote_resolved_models() -> None:
    mgr = _mgr()
    mgr.tunnel_http = AsyncMock(  # type: ignore[method-assign]
        return_value=_json_response(
            [{"provider_name": "openai", "model": "gpt-4o", "enabled": True}]
        )
    )
    out = await mgr.list_remote_resolved_models("cid1", owner_user_id=1)
    assert out[0]["model"] == "gpt-4o"
    mgr.tunnel_http.assert_awaited_once_with(
        connection_id="cid1",
        owner_user_id=1,
        method="GET",
        path="/api/providers/resolved",
        query="",
    )


@pytest.mark.asyncio
async def test_list_remote_knowledge_bases() -> None:
    mgr = _mgr()
    mgr.tunnel_http = AsyncMock(  # type: ignore[method-assign]
        return_value=_json_response([{"id": "kb_1", "name": "Docs"}])
    )
    out = await mgr.list_remote_knowledge_bases("cid1", owner_user_id=1)
    assert out[0]["id"] == "kb_1"
    mgr.tunnel_http.assert_awaited_once_with(
        connection_id="cid1",
        owner_user_id=1,
        method="GET",
        path="/api/knowledge-bases",
        query="",
    )


@pytest.mark.asyncio
async def test_tunnel_json_get_rejects_http_error() -> None:
    mgr = _mgr()
    mgr.tunnel_http = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(502, text="boom")
    )
    with pytest.raises(OctopError) as exc:
        await mgr.get_remote_knowledge_capability("cid1", owner_user_id=1)
    assert exc.value.code == ErrorCode.BRIDGE_TUNNEL_FAILED
