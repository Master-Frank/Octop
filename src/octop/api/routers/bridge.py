"""HTTP API for Bridge connection management + inbound WS."""

from __future__ import annotations

import logging
from typing import Any, cast

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from octop.api.deps import get_server, require_permission, resolve_user_from_token
from octop.infra.errors import ErrorCode, OctopError

logger = logging.getLogger(__name__)

router = APIRouter()

# Management surface matches Advanced → Bridge (admin_console). Inbound WS
# stays available to any authenticated user so a peer can dial a non-admin
# account if that account owns the reverse link.
_require_bridge_admin = require_permission("admin_console")


class BridgeCreateBody(BaseModel):
    peer_base_url: str = Field(..., description="Remote Octop base URL, e.g. https://cloud.example")
    peer_username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)
    display_name: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Unique display name for chat group switching",
    )
    notes: str | None = Field(default=None, max_length=500, description="Optional notes")
    connect: bool = Field(default=True, description="Dial Bridge WS immediately after login")


class BridgeProbeBody(BaseModel):
    peer_base_url: str = Field(..., description="Remote Octop base URL")
    peer_username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


def _bridge(server: Any) -> Any:
    rt = server.app_runtime
    if rt is None or getattr(rt, "bridge_manager", None) is None:
        raise OctopError(ErrorCode.INTERNAL_ERROR, "bridge not ready")
    return rt.bridge_manager


@router.post("/bridge/probe", summary="Probe a remote Octop (login + list experts)")
async def probe_peer(
    body: BridgeProbeBody,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Validate remote credentials and return that account's expert list.

    Does not create a bridge connection or open the Bridge WebSocket.
    """
    _ = user
    mgr = _bridge(server)
    return cast(
        dict[str, Any],
        await mgr.probe_peer(
            peer_base_url=body.peer_base_url,
            peer_username=body.peer_username,
            password=body.password,
        ),
    )


@router.get("/bridge/connections", summary="List bridge connections")
async def list_connections(
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    mgr = _bridge(server)
    rows = mgr.list_connections(user.id)
    return [cast(dict[str, Any], mgr.connection_public(r)) for r in rows]


@router.post("/bridge/connections", status_code=201, summary="Add a bridge connection")
async def create_connection(
    body: BridgeCreateBody,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    mgr = _bridge(server)
    row = await mgr.create_connection(
        owner_user_id=user.id,
        peer_base_url=body.peer_base_url,
        peer_username=body.peer_username,
        password=body.password,
        display_name=body.display_name,
        notes=body.notes,
        connect=body.connect,
    )
    return cast(dict[str, Any], mgr.connection_public(row))


@router.get("/bridge/connections/{connection_id}", summary="Get a bridge connection")
async def get_connection(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    mgr = _bridge(server)
    row = mgr.get_owned(connection_id, user.id)
    return cast(dict[str, Any], mgr.connection_public(row))


@router.post(
    "/bridge/connections/{connection_id}/connect",
    summary="Connect / reconnect Bridge WS",
)
async def connect_connection(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    mgr = _bridge(server)
    row = await mgr.connect(connection_id, owner_user_id=user.id)
    return cast(dict[str, Any], mgr.connection_public(row))


@router.post(
    "/bridge/connections/{connection_id}/disconnect",
    summary="Disconnect Bridge WS",
)
async def disconnect_connection(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    mgr = _bridge(server)
    mgr.get_owned(connection_id, user.id)
    await mgr.disconnect(connection_id)
    row = mgr.get_owned(connection_id, user.id)
    return cast(dict[str, Any], mgr.connection_public(row))


class BridgePatchBody(BaseModel):
    auto_reconnect: bool | None = Field(
        default=None,
        description="When true, dial again after unexpected disconnect (default on)",
    )


@router.patch(
    "/bridge/connections/{connection_id}",
    summary="Update bridge connection settings",
)
async def patch_connection(
    connection_id: str,
    body: BridgePatchBody,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    mgr = _bridge(server)
    if body.auto_reconnect is None:
        row = mgr.get_owned(connection_id, user.id)
        return cast(dict[str, Any], mgr.connection_public(row))
    row = await mgr.set_auto_reconnect(
        connection_id, owner_user_id=user.id, enabled=bool(body.auto_reconnect)
    )
    return cast(dict[str, Any], mgr.connection_public(row))


@router.delete(
    "/bridge/connections/{connection_id}",
    status_code=204,
    summary="Delete a bridge connection",
)
async def delete_connection(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> None:
    mgr = _bridge(server)
    await mgr.delete_connection(connection_id, owner_user_id=user.id)


@router.get(
    "/bridge/connections/{connection_id}/agents",
    summary="List remote agents via bridge",
)
async def list_remote_agents(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    mgr = _bridge(server)
    agents = await mgr.list_remote_agents(connection_id, owner_user_id=user.id)
    return [cast(dict[str, Any], item) for item in agents]


@router.get(
    "/bridge/connections/{connection_id}/providers/resolved",
    summary="List peer resolved models via bridge tunnel",
)
async def list_remote_resolved_models(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    """Read-only: peer ``GET /api/providers/resolved`` for remote chat model picker."""
    mgr = _bridge(server)
    models = await mgr.list_remote_resolved_models(connection_id, owner_user_id=user.id)
    return [cast(dict[str, Any], item) for item in models]


@router.get(
    "/bridge/connections/{connection_id}/providers/active-model",
    summary="Get peer active model via bridge tunnel",
)
async def get_remote_active_model(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Read-only: peer ``GET /api/providers/active-model``."""
    mgr = _bridge(server)
    return cast(
        dict[str, Any],
        await mgr.get_remote_active_model(connection_id, owner_user_id=user.id),
    )


@router.get(
    "/bridge/connections/{connection_id}/knowledge-bases",
    summary="List peer knowledge bases via bridge tunnel",
)
async def list_remote_knowledge_bases(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    """Read-only: peer ``GET /api/knowledge-bases`` for remote chat KB picker."""
    mgr = _bridge(server)
    bases = await mgr.list_remote_knowledge_bases(connection_id, owner_user_id=user.id)
    return [cast(dict[str, Any], item) for item in bases]


@router.get(
    "/bridge/connections/{connection_id}/knowledge-bases/capability",
    summary="Get peer knowledge capability via bridge tunnel",
)
async def get_remote_knowledge_capability(
    connection_id: str,
    user: Any = Depends(_require_bridge_admin),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Read-only: peer ``GET /api/knowledge-bases/capability``."""
    mgr = _bridge(server)
    return cast(
        dict[str, Any],
        await mgr.get_remote_knowledge_capability(connection_id, owner_user_id=user.id),
    )


@router.websocket("/bridge/ws")
async def bridge_inbound_ws(
    websocket: WebSocket,
) -> None:
    """Peer Octop dials in; JWT is the peer user's token on this instance."""
    server = websocket.app.state.octop_server
    raw = websocket.query_params.get("token")
    if not raw:
        await websocket.close(code=4001, reason="missing token")
        return
    try:
        user = resolve_user_from_token(server, raw)
    except OctopError as exc:
        await websocket.close(code=4001, reason=f"auth: {exc.code.value}")
        return
    mgr = getattr(getattr(server, "app_runtime", None), "bridge_manager", None)
    if mgr is None:
        await websocket.close(code=1011, reason="bridge not ready")
        return
    await websocket.accept()
    try:
        await mgr.accept_inbound(websocket=websocket, user=user)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("bridge inbound ws error")
