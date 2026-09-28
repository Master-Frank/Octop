"""Proxy HTTP requests for ``bridge:{connection_id}:{agent_id}`` agent paths."""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request
from fastapi.responses import Response

from octop.infra.bridge.ids import BRIDGE_AGENT_PREFIX, parse_bridge_agent_id
from octop.infra.errors import OctopError

logger = logging.getLogger(__name__)

_INSTALL_ATTR = "_octop_bridge_proxy_installed"

# /api/agents/bridge:{cid}:{aid} or percent-encoded bridge%3A…
_BRIDGE_AGENT_PATH = re.compile(
    r"^/api/agents/(bridge(?::|%3[Aa])[^/]+)(/.*)?$",
)


def install(app: Any, server: Any) -> None:
    if getattr(app, _INSTALL_ATTR, False):
        return
    setattr(app, _INSTALL_ATTR, True)

    @app.middleware("http")  # type: ignore[untyped-decorator]
    async def _bridge_proxy(
        request: Request,
        call_next: Callable[[Request], Awaitable[Any]],
    ) -> Any:
        path = request.url.path
        match = _BRIDGE_AGENT_PATH.match(path)
        if match is None:
            return await call_next(request)
        # WebSocket upgrades are handled by the chat/bridge routers.
        if (request.headers.get("connection") or "").lower() == "upgrade":
            return await call_next(request)

        agent_token = match.group(1)
        rest = match.group(2) or ""
        # Starlette usually decodes; still normalize percent-encoded ``:``.
        from urllib.parse import unquote

        agent_token = unquote(agent_token)
        ref = parse_bridge_agent_id(agent_token)
        if ref is None:
            return await call_next(request)

        user = getattr(request.state, "octop_user", None)
        rt = getattr(server, "app_runtime", None)
        mgr = getattr(rt, "bridge_manager", None) if rt is not None else None
        if user is None or mgr is None:
            return await call_next(request)

        remote_path = f"/api/agents/{ref.remote_agent_id}{rest}"
        body = await request.body()
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower()
            not in {
                "host",
                "content-length",
                "authorization",
                "connection",
                "transfer-encoding",
            }
        }
        try:
            peer_resp = await mgr.tunnel_http(
                connection_id=ref.connection_id,
                owner_user_id=int(user.id),
                method=request.method,
                path=remote_path,
                query=request.url.query,
                headers=headers,
                body=body or None,
            )
        except OctopError as exc:
            from fastapi.responses import JSONResponse

            return JSONResponse(status_code=exc.status, content=exc.to_envelope())
        except Exception:
            logger.exception("bridge proxy failed path=%s", path)
            from fastapi.responses import JSONResponse

            return JSONResponse(
                status_code=502,
                content={
                    "error": {"code": "BRIDGE_TUNNEL_FAILED", "message": "bridge proxy failed"}
                },
            )

        # Rewrite absolute preview URLs that embed the remote agent id when present
        content = peer_resp.content
        media = peer_resp.headers.get("content-type", "")
        if "json" in media and BRIDGE_AGENT_PREFIX.encode() not in content:
            # Remap remote agent id strings in JSON bodies back to bridge ids
            try:
                text = content.decode("utf-8")
                if ref.remote_agent_id in text:
                    text = text.replace(
                        ref.remote_agent_id,
                        agent_token,
                    )
                    content = text.encode("utf-8")
            except UnicodeDecodeError:
                pass

        out_headers = {
            k: v
            for k, v in peer_resp.headers.items()
            if k.lower()
            not in {
                "content-length",
                "transfer-encoding",
                "connection",
                "content-encoding",
            }
        }
        return Response(
            content=content,
            status_code=peer_resp.status_code,
            headers=out_headers,
            media_type=peer_resp.headers.get("content-type"),
        )
