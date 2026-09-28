"""BridgeManager — connection lifecycle, live sessions, HTTP tunnel, turn relay."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx
import websockets

from octop.infra.bridge.crypto import decrypt_payload, encrypt_payload
from octop.infra.bridge.http_tunnel import decode_body_b64, execute_local_http
from octop.infra.bridge.ids import PROTOCOL_VERSION, format_bridge_agent_id
from octop.infra.bridge.peer_auth import login_peer, normalize_peer_base_url, peer_ws_url
from octop.infra.bridge.transport import BridgeSession
from octop.infra.db.repos.bridge_connections import BridgeConnectionRepo, BridgeConnectionRow
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.utils.ulid import new_ulid

logger = logging.getLogger(__name__)


def _absolute_peer_url(base_url: str, maybe_path: str | None) -> str | None:
    raw = (maybe_path or "").strip()
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    root = base_url.rstrip("/") + "/"
    return urljoin(root, raw.lstrip("/"))


def _probe_agent_summary(item: dict[str, Any], *, peer_base_url: str) -> dict[str, Any]:
    remote_id = str(item.get("agent_id") or item.get("id") or "").strip()
    icon_url = _absolute_peer_url(
        peer_base_url,
        str(item.get("icon_url") or item.get("icon") or "").strip() or None,
    )
    return {
        "agent_id": remote_id,
        "name": str(item.get("name") or remote_id or "").strip() or remote_id,
        "description": str(item.get("description") or "").strip() or None,
        "icon_url": icon_url,
        "icon_name": item.get("icon_name"),
        "color": item.get("color"),
        "state": item.get("state"),
        "kind": item.get("kind") or "expert",
    }


class BridgeManager:
    def __init__(
        self,
        *,
        bridge_repo: BridgeConnectionRepo,
        secret_repo: SecretRepo,
        user_repo: UserRepo,
        advertise_base_url: str,
        token_signer: Any | None = None,
    ) -> None:
        self._repo = bridge_repo
        self._secrets = secret_repo
        self._users = user_repo
        self._advertise_base_url = advertise_base_url.rstrip("/")
        self._token_signer = token_signer
        self._sessions: dict[str, BridgeSession] = {}
        self._client_tasks: dict[str, asyncio.Task[None]] = {}
        self._turn_waiters: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._asgi_app: Any | None = None
        self._lock = asyncio.Lock()

    def bind_asgi_app(self, app: Any) -> None:
        self._asgi_app = app

    def set_token_signer(self, signer: Any) -> None:
        self._token_signer = signer

    def set_advertise_base_url(self, url: str) -> None:
        self._advertise_base_url = url.rstrip("/")

    # -- persistence helpers -------------------------------------------------

    def list_connections(self, owner_user_id: int) -> list[BridgeConnectionRow]:
        return self._repo.list_for_owner(owner_user_id)

    def get_owned(self, connection_id: str, owner_user_id: int) -> BridgeConnectionRow:
        row = self._repo.get_for_owner(connection_id, owner_user_id)
        if row is None:
            raise OctopError(ErrorCode.BRIDGE_NOT_FOUND, "bridge connection not found")
        return row

    def _allocate_display_name(self, *, owner_user_id: int, preferred: str) -> str:
        base = preferred.strip() or "Remote"
        candidate = base
        n = 2
        while self._repo.find_by_display_name(owner_user_id, candidate) is not None:
            candidate = f"{base} ({n})"
            n += 1
        return candidate

    def connection_public(self, row: BridgeConnectionRow) -> dict[str, Any]:
        live = self._sessions.get(row.connection_id)
        status = row.status
        if live is not None and not live.closed:
            status = "connected"
        elif status == "connected":
            status = "disconnected"
        return {
            "connection_id": row.connection_id,
            "peer_base_url": row.peer_base_url,
            "peer_username": row.peer_username,
            "display_name": row.display_name,
            "notes": row.notes,
            "status": status,
            "last_error": row.last_error,
            "last_seen_at": row.last_seen_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "has_password": bool(row.credential_blob),
        }

    async def probe_peer(
        self,
        *,
        peer_base_url: str,
        peer_username: str,
        password: str,
    ) -> dict[str, Any]:
        """Login to peer over HTTP and list experts (no connection row / WS)."""
        base = normalize_peer_base_url(peer_base_url)
        username = peer_username.strip()
        if not username or not password:
            raise OctopError(ErrorCode.BRIDGE_AUTH_FAILED, "username and password required")
        login = await login_peer(base_url=base, username=username, password=password)
        token = str(login["access_token"])
        raw_user = login.get("user")
        user_info: dict[str, Any] = raw_user if isinstance(raw_user, dict) else {}
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                resp = await client.get(
                    urljoin(base + "/", "api/agents"),
                    params={"scope": "mine"},
                    headers={"Authorization": f"Bearer {token}"},
                )
        except httpx.HTTPError as exc:
            raise OctopError(
                ErrorCode.BRIDGE_PEER_UNREACHABLE,
                f"peer agents probe failed: {exc}",
            ) from exc
        if resp.status_code >= 400:
            raise OctopError(
                ErrorCode.BRIDGE_AUTH_FAILED,
                f"peer agents HTTP {resp.status_code}: {resp.text[:200]}",
            )
        try:
            data = resp.json()
        except ValueError as exc:
            raise OctopError(
                ErrorCode.BRIDGE_TUNNEL_FAILED,
                "peer agents returned non-JSON",
            ) from exc
        if not isinstance(data, list):
            raise OctopError(ErrorCode.BRIDGE_TUNNEL_FAILED, "peer agents payload invalid")
        agents = [
            _probe_agent_summary(item, peer_base_url=base)
            for item in data
            if isinstance(item, dict) and str(item.get("agent_id") or item.get("id") or "").strip()
        ]
        return {
            "peer_base_url": base,
            "peer_username": username,
            "peer_display_name": (
                str(user_info.get("display_name") or "").strip()
                or str(user_info.get("username") or username).strip()
            ),
            "agent_count": len(agents),
            "agents": agents,
        }

    # -- create / delete / connect -------------------------------------------

    async def create_connection(
        self,
        *,
        owner_user_id: int,
        peer_base_url: str,
        peer_username: str,
        password: str,
        display_name: str,
        notes: str | None = None,
        connect: bool = True,
    ) -> BridgeConnectionRow:
        """Create a bridge link.

        With ``connect=True`` (Dashboard default), the row is only kept when the
        peer login **and** Bridge WS handshake both succeed. A failed dial rolls
        back the insert so a retry can reuse the same display name.
        """
        base = normalize_peer_base_url(peer_base_url)
        username = peer_username.strip()
        name = display_name.strip()
        note = (notes or "").strip() or None
        if not username or not password:
            raise OctopError(ErrorCode.BRIDGE_AUTH_FAILED, "username and password required")
        if not name:
            raise OctopError(
                ErrorCode.FORBIDDEN,
                "display_name required",
                status=400,
                details={"field": "display_name"},
            )
        if self._repo.find_by_display_name(owner_user_id, name) is not None:
            raise OctopError(
                ErrorCode.BRIDGE_DISPLAY_NAME_TAKEN,
                f"display name {name!r} already in use",
                details={"name": name},
            )

        # Login first — no DB row yet on auth / unreachable peer.
        login = await login_peer(base_url=base, username=username, password=password)
        token = str(login["access_token"])
        expires_in = int(login.get("expires_in") or 0)
        from octop.infra.db.repos._base import now_ts

        expires_at = now_ts() + expires_in if expires_in > 0 else None
        connection_id = new_ulid()
        cred_blob = encrypt_payload(self._secrets, {"password": password})
        token_blob = encrypt_payload(self._secrets, {"access_token": token})
        row = self._repo.create(
            connection_id=connection_id,
            owner_user_id=owner_user_id,
            peer_base_url=base,
            peer_username=username,
            display_name=name,
            notes=note,
            credential_blob=cred_blob,
            access_token_blob=token_blob,
            token_expires_at=expires_at,
            status="connecting" if connect else "disconnected",
        )
        if not connect:
            return self._repo.get(connection_id) or row
        try:
            return await self.connect(connection_id, owner_user_id=owner_user_id)
        except Exception:
            # Roll back so a failed "Save & connect" does not occupy display_name
            # or leave a dead card in the list.
            with suppress(Exception):
                await self.disconnect(connection_id)
            self._repo.delete(connection_id)
            raise

    async def delete_connection(self, connection_id: str, *, owner_user_id: int) -> None:
        self.get_owned(connection_id, owner_user_id)
        await self.disconnect(connection_id)
        self._repo.delete(connection_id)

    async def connect(self, connection_id: str, *, owner_user_id: int) -> BridgeConnectionRow:
        row = self.get_owned(connection_id, owner_user_id)
        async with self._lock:
            existing = self._sessions.get(connection_id)
            if existing is not None and not existing.closed:
                return row
            self._repo.update_status(connection_id, status="connecting", last_error=None)
            token = await self._ensure_peer_token(row)
            task = asyncio.create_task(
                self._run_outbound_client(connection_id, row.peer_base_url, token),
                name=f"bridge-out-{connection_id}",
            )
            self._client_tasks[connection_id] = task
        # Wait briefly for hello_ack
        for _ in range(50):
            sess = self._sessions.get(connection_id)
            if sess is not None and not sess.closed:
                self._repo.update_status(
                    connection_id, status="connected", last_error=None, touch_seen=True
                )
                return self._repo.get(connection_id) or row
            if task.done():
                exc = task.exception() if not task.cancelled() else None
                msg = str(exc) if exc else "bridge connect failed"
                self._repo.update_status(connection_id, status="error", last_error=msg[:500])
                raise OctopError(ErrorCode.BRIDGE_PEER_UNREACHABLE, msg)
            await asyncio.sleep(0.1)
        self._repo.update_status(connection_id, status="error", last_error="bridge connect timeout")
        raise OctopError(ErrorCode.BRIDGE_PEER_UNREACHABLE, "bridge connect timeout")

    async def disconnect(self, connection_id: str) -> None:
        task = self._client_tasks.pop(connection_id, None)
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        sess = self._sessions.pop(connection_id, None)
        if sess is not None:
            await sess.close()
        if self._repo.get(connection_id) is not None:
            self._repo.update_status(connection_id, status="disconnected")

    async def _ensure_peer_token(self, row: BridgeConnectionRow) -> str:
        from octop.infra.db.repos._base import now_ts

        if row.access_token_blob:
            data = decrypt_payload(self._secrets, row.access_token_blob)
            token = str(data.get("access_token") or "").strip()
            exp = row.token_expires_at
            if token and (exp is None or exp > now_ts() + 60):
                return token
        if not row.credential_blob:
            raise OctopError(ErrorCode.BRIDGE_AUTH_FAILED, "no peer credentials stored")
        creds = decrypt_payload(self._secrets, row.credential_blob)
        password = str(creds.get("password") or "")
        login = await login_peer(
            base_url=row.peer_base_url,
            username=row.peer_username,
            password=password,
        )
        token = str(login["access_token"])
        expires_in = int(login.get("expires_in") or 0)
        expires_at = now_ts() + expires_in if expires_in > 0 else None
        self._repo.update_credentials(
            row.connection_id,
            access_token_blob=encrypt_payload(self._secrets, {"access_token": token}),
            token_expires_at=expires_at,
        )
        return token

    async def _run_outbound_client(
        self, connection_id: str, peer_base_url: str, token: str
    ) -> None:
        url = peer_ws_url(peer_base_url, token=token)
        session: BridgeSession | None = None
        try:
            async with websockets.connect(url, open_timeout=20, max_size=32 * 1024 * 1024) as ws:
                owner = self._repo.get(connection_id)
                local_user = self._users.get(owner.owner_user_id) if owner is not None else None
                hello = {
                    "type": "hello",
                    "protocol_version": PROTOCOL_VERSION,
                    "connection_id": connection_id,
                    "role": "initiator",
                    "advertise_base_url": self._advertise_base_url or "http://127.0.0.1",
                    "advertise_username": (local_user.username if local_user is not None else ""),
                    "display_name": owner.display_name if owner else None,
                }
                await ws.send(json.dumps(hello, ensure_ascii=False))

                # Wait for hello_ack before advertising the session as connected.
                while True:
                    raw_ack = await asyncio.wait_for(ws.recv(), timeout=20.0)
                    text_ack = (
                        raw_ack
                        if isinstance(raw_ack, str)
                        else raw_ack.decode("utf-8", errors="replace")
                    )
                    try:
                        ack = json.loads(text_ack)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(ack, dict):
                        continue
                    if str(ack.get("type") or "") != "hello_ack":
                        continue
                    peer_version = int(ack.get("protocol_version") or 0)
                    if peer_version != PROTOCOL_VERSION:
                        raise OctopError(
                            ErrorCode.BRIDGE_PEER_UNREACHABLE,
                            f"bridge protocol mismatch: peer={peer_version} local={PROTOCOL_VERSION}",
                        )
                    break

                session = BridgeSession(
                    connection_id=connection_id,
                    send_text=ws.send,
                    on_tunnel_request=lambda p: self._handle_inbound_tunnel(connection_id, p),
                    on_turn_frame=lambda p: self._handle_inbound_turn(connection_id, p),
                )
                await self._register_session(connection_id, session)
                async for raw in ws:
                    text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                    await session.handle_message(text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("bridge outbound closed connection=%s: %s", connection_id, exc)
            detail = str(exc)
            # Peer Octop without Bridge returns HTTP 403/404 on /api/bridge/ws.
            low = detail.lower()
            if "403" in detail or "404" in detail or "rejected websocket" in low:
                detail = (
                    f"{detail}; peer may not support Bridge yet (needs Octop with /api/bridge/ws)"
                )
            if self._repo.get(connection_id) is not None:
                self._repo.update_status(
                    connection_id, status="disconnected", last_error=detail[:500]
                )
            raise
        finally:
            await self._unregister_session(connection_id)

    # -- inbound WS (peer dials us) ------------------------------------------

    async def accept_inbound(
        self,
        *,
        websocket: Any,
        user: Any,
    ) -> None:
        """Handle an inbound ``/api/bridge/ws`` after JWT auth + accept()."""
        connection_id: str | None = None
        session: BridgeSession | None = None

        async def send_text(text: str) -> None:
            await websocket.send_text(text)

        try:
            raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            payload = json.loads(raw)
        except Exception:
            await websocket.close(code=4000, reason="hello required")
            return
        if not isinstance(payload, dict) or str(payload.get("type") or "") != "hello":
            await websocket.close(code=4000, reason="hello required")
            return
        peer_version = int(payload.get("protocol_version") or 0)
        if peer_version != PROTOCOL_VERSION:
            await websocket.close(code=4002, reason="protocol mismatch")
            return
        connection_id = str(payload.get("connection_id") or "").strip()
        if not connection_id:
            await websocket.close(code=4000, reason="connection_id required")
            return
        advertise_base = str(payload.get("advertise_base_url") or "").strip() or "http://127.0.0.1"
        advertise_user = str(payload.get("advertise_username") or "").strip() or "peer"
        preferred_name = str(payload.get("display_name") or "").strip()
        try:
            advertise_base = normalize_peer_base_url(advertise_base)
        except OctopError:
            advertise_base = "http://127.0.0.1"

        existing = self._repo.get(connection_id)
        if existing is not None and int(existing.owner_user_id) != int(user.id):
            await websocket.close(code=4003, reason="connection owned by another user")
            return
        if existing is None:
            display_name = self._allocate_display_name(
                owner_user_id=int(user.id),
                preferred=preferred_name or advertise_user or advertise_base,
            )
        else:
            display_name = existing.display_name

        self._repo.upsert_reverse(
            connection_id=connection_id,
            owner_user_id=int(user.id),
            peer_base_url=advertise_base,
            peer_username=advertise_user,
            display_name=display_name,
            status="connected",
        )

        session = BridgeSession(
            connection_id=connection_id,
            send_text=send_text,
            on_tunnel_request=lambda p: self._handle_inbound_tunnel(connection_id, p),
            on_turn_frame=lambda p: self._handle_inbound_turn(connection_id, p),
        )
        await self._register_session(connection_id, session)
        await session.send_json(
            {
                "type": "hello_ack",
                "protocol_version": PROTOCOL_VERSION,
                "connection_id": connection_id,
                "peer_username": user.username,
            }
        )
        try:
            while True:
                message = await websocket.receive_text()
                await session.handle_message(message)
        except Exception:
            logger.info("bridge inbound closed connection=%s", connection_id)
        finally:
            if connection_id:
                await self._unregister_session(connection_id)
                self._repo.update_status(connection_id, status="disconnected")

    async def _register_session(self, connection_id: str, session: BridgeSession) -> None:
        old = self._sessions.get(connection_id)
        if old is not None and old is not session:
            await old.close()
        self._sessions[connection_id] = session

    async def _unregister_session(self, connection_id: str) -> None:
        sess = self._sessions.pop(connection_id, None)
        if sess is not None:
            await sess.close()

    # -- tunnel --------------------------------------------------------------

    def require_session(self, connection_id: str) -> BridgeSession:
        sess = self._sessions.get(connection_id)
        if sess is None or sess.closed:
            raise OctopError(ErrorCode.BRIDGE_NOT_CONNECTED, "bridge not connected")
        return sess

    async def tunnel_http(
        self,
        *,
        connection_id: str,
        owner_user_id: int,
        method: str,
        path: str,
        query: str = "",
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> httpx.Response:
        self.get_owned(connection_id, owner_user_id)
        sess = self.require_session(connection_id)
        result = await sess.tunnel_request(
            method=method,
            path=path,
            query=query,
            headers=headers,
            body=body,
        )
        if str(result.get("type") or "") == "tunnel.error":
            raise OctopError(
                ErrorCode.BRIDGE_TUNNEL_FAILED,
                str(result.get("message") or "tunnel error"),
            )
        status = int(result.get("status") or 502)
        hdrs = result.get("headers") if isinstance(result.get("headers"), dict) else {}
        content = decode_body_b64(result)
        return httpx.Response(status, headers=hdrs, content=content)

    async def _owner_access_token(self, connection_id: str) -> str:
        row = self._repo.get(connection_id)
        if row is None:
            raise OctopError(ErrorCode.BRIDGE_NOT_FOUND, "bridge connection not found")
        if self._token_signer is None:
            raise OctopError(ErrorCode.INTERNAL_ERROR, "bridge token signer not configured")
        user = self._users.get(row.owner_user_id)
        if user is None:
            raise OctopError(ErrorCode.BRIDGE_NOT_FOUND, "bridge owner missing")
        return str(self._token_signer(user))

    async def _handle_inbound_tunnel(
        self, connection_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if self._asgi_app is None:
            raise RuntimeError("ASGI app not bound")
        token = await self._owner_access_token(connection_id)
        method = str(payload.get("method") or "GET")
        path = str(payload.get("path") or "/")
        query = str(payload.get("query") or "")
        headers_raw = payload.get("headers")
        headers: dict[str, Any] = headers_raw if isinstance(headers_raw, dict) else {}
        body = decode_body_b64(payload)
        return await execute_local_http(
            app=self._asgi_app,
            method=method,
            path=path,
            query=query,
            headers={str(k): str(v) for k, v in headers.items()},
            body=body,
            access_token=token,
        )

    # -- agents via tunnel ---------------------------------------------------

    async def list_remote_agents(
        self, connection_id: str, *, owner_user_id: int
    ) -> list[dict[str, Any]]:
        row = self.get_owned(connection_id, owner_user_id)
        resp = await self.tunnel_http(
            connection_id=connection_id,
            owner_user_id=owner_user_id,
            method="GET",
            path="/api/agents",
            query="scope=mine",
        )
        if resp.status_code >= 400:
            raise OctopError(
                ErrorCode.BRIDGE_TUNNEL_FAILED,
                f"peer agents HTTP {resp.status_code}: {resp.text[:200]}",
            )
        data = resp.json()
        if not isinstance(data, list):
            raise OctopError(ErrorCode.BRIDGE_TUNNEL_FAILED, "peer agents payload invalid")
        out: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            # Prefer public agent_id; integer ``id`` is the DB surrogate PK.
            remote_id = str(item.get("agent_id") or item.get("id") or "").strip()
            if not remote_id:
                continue
            mapped = dict(item)
            mapped["id"] = format_bridge_agent_id(connection_id, remote_id)
            mapped["agent_id"] = mapped["id"]
            mapped["bridge_connection_id"] = connection_id
            mapped["bridge_connection_name"] = row.display_name
            mapped["remote_agent_id"] = remote_id
            mapped["bridge"] = True
            # Shadow experts are chat-ready while the bridge link is live.
            mapped["state"] = "running"
            # Serve icons via local proxy so the browser never hits the peer.
            if str(mapped.get("icon_url") or mapped.get("icon") or "").strip():
                from octop.infra.agents.experts.avatar import agent_avatar_api_path

                mapped["icon_url"] = agent_avatar_api_path(mapped["id"])
            out.append(mapped)
        return out

    # -- chat turn relay -----------------------------------------------------

    async def open_turn_waiter(self, request_id: str) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._turn_waiters[request_id] = q
        return q

    def close_turn_waiter(self, request_id: str) -> None:
        self._turn_waiters.pop(request_id, None)

    async def _handle_inbound_turn(self, connection_id: str, payload: dict[str, Any]) -> None:
        msg_type = str(payload.get("type") or "")
        request_id = str(payload.get("request_id") or "").strip()
        if msg_type in {"turn.chunk", "turn.end", "turn.error"} and request_id:
            q = self._turn_waiters.get(request_id)
            if q is not None:
                await q.put(payload)
            return
        if msg_type == "turn.start":
            await self._execute_peer_turn(connection_id, payload)

    async def _execute_peer_turn(self, connection_id: str, payload: dict[str, Any]) -> None:
        """Peer asked us to run a turn on a local agent; stream chunks back."""
        from octop.api.routers.chat.models import UserTurnWsFrame
        from octop.api.routers.chat.turn import (
            build_dashboard_inbound,
            prepare_dashboard_turn,
            turn_has_content,
        )
        from octop.infra.gateway.ws import WS_CHANNEL_ID
        from octop.infra.gateway.ws.ws_hub import stamp_thread_id

        request_id = str(payload.get("request_id") or "").strip()
        agent_id = str(payload.get("agent_id") or "").strip()
        sess = self._sessions.get(connection_id)
        if sess is None or not request_id or not agent_id:
            return
        row = self._repo.get(connection_id)
        if row is None:
            return
        if self._asgi_app is None:
            await sess.send_json(
                {
                    "type": "turn.error",
                    "request_id": request_id,
                    "message": "bridge owner unavailable",
                }
            )
            return
        server = self._asgi_app.state.octop_server
        user = (
            server.app_runtime.user_manager.get_by_id(row.owner_user_id)
            if server.app_runtime is not None
            else None
        )
        if user is None:
            await sess.send_json(
                {
                    "type": "turn.error",
                    "request_id": request_id,
                    "message": "bridge owner unavailable",
                }
            )
            return
        assert server.app_runtime is not None
        gateway = server.app_runtime.gateway
        hub = gateway.ws_hub
        channel_manager = gateway.channel_manager
        if channel_manager is None:
            await sess.send_json(
                {
                    "type": "turn.error",
                    "request_id": request_id,
                    "message": "gateway not ready",
                }
            )
            return

        bridge_conn_id = f"bridge-turn-{request_id}"
        chunk_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

        async def send_frame(frame: dict[str, Any]) -> None:
            await chunk_queue.put(frame)

        hub.register(bridge_conn_id, send_frame, user_id=user.id)
        try:
            frame = UserTurnWsFrame.model_validate(
                {
                    "type": "user_turn",
                    "text": payload.get("text") or "",
                    "thread_id": payload.get("thread_id"),
                    "messages": payload.get("messages"),
                    "attachments": payload.get("attachments"),
                    "mcp_servers": payload.get("mcp_servers"),
                    "skills": payload.get("skills"),
                    "model": payload.get("model"),
                }
            )
            turn = frame.to_turn_body()
            if not turn_has_content(turn):
                await sess.send_json(
                    {
                        "type": "turn.error",
                        "request_id": request_id,
                        "message": "empty message",
                    }
                )
                return
            prepared = await prepare_dashboard_turn(
                server,
                agent_id=agent_id,
                user=user,
                turn=turn,
            )
            inbound = build_dashboard_inbound(
                agent_id=agent_id,
                user_id=user.id,
                prepared=prepared,
                turn=turn,
                ws_connection_id=bridge_conn_id,
                user_is_admin=bool(getattr(user, "is_admin", False)),
            )
            hub.subscribe(prepared.thread_id, bridge_conn_id)
            await sess.send_json(
                {
                    "type": "turn.chunk",
                    "request_id": request_id,
                    "frame": {
                        "type": "thread",
                        "thread_id": prepared.thread_id,
                    },
                }
            )
            channel_manager.enqueue(WS_CHANNEL_ID, inbound)
            while True:
                item = await chunk_queue.get()
                if item is None:
                    break
                stamped = stamp_thread_id(item, prepared.thread_id)
                await sess.send_json(
                    {
                        "type": "turn.chunk",
                        "request_id": request_id,
                        "frame": stamped,
                    }
                )
                if str(stamped.get("type") or "") in {"done", "error"}:
                    break
            await sess.send_json({"type": "turn.end", "request_id": request_id})
        except Exception as exc:
            logger.exception("bridge peer turn failed")
            await sess.send_json(
                {
                    "type": "turn.error",
                    "request_id": request_id,
                    "message": str(exc)[:500],
                }
            )
        finally:
            hub.unregister(bridge_conn_id)

    async def relay_user_turn(
        self,
        *,
        connection_id: str,
        owner_user_id: int,
        remote_agent_id: str,
        turn_payload: dict[str, Any],
        on_frame: Any,
    ) -> None:
        """Send turn.start to peer and forward turn.chunk frames to ``on_frame``."""
        import uuid

        self.get_owned(connection_id, owner_user_id)
        sess = self.require_session(connection_id)
        request_id = uuid.uuid4().hex
        queue = await self.open_turn_waiter(request_id)
        try:
            await sess.send_json(
                {
                    "type": "turn.start",
                    "request_id": request_id,
                    "agent_id": remote_agent_id,
                    **turn_payload,
                }
            )
            while True:
                msg = await asyncio.wait_for(queue.get(), timeout=600.0)
                msg_type = str(msg.get("type") or "")
                if msg_type == "turn.chunk":
                    frame = msg.get("frame")
                    if isinstance(frame, dict):
                        await on_frame(frame)
                elif msg_type == "turn.error":
                    await on_frame(
                        {
                            "type": "error",
                            "message": str(msg.get("message") or "bridge turn error"),
                        }
                    )
                    await on_frame({"type": "done"})
                    break
                elif msg_type == "turn.end":
                    break
        finally:
            self.close_turn_waiter(request_id)


def public_base_url_from_config(bind_host: str, port: int) -> str:
    host = bind_host if bind_host not in {"0.0.0.0", "::"} else "127.0.0.1"
    return f"http://{host}:{port}"


def split_request_path(path: str) -> tuple[str, str]:
    """Return (path_without_query, query)."""
    parts = urlsplit(path)
    return parts.path, parts.query
