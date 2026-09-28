"""Unit tests for BridgeManager.update_connection_meta endpoint edits."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from octop.infra.bridge.manager import BridgeManager
from octop.infra.db.repos.bridge_connections import BridgeConnectionRow
from octop.infra.errors import ErrorCode, OctopError


def _row(**overrides: Any) -> BridgeConnectionRow:
    base = {
        "pk": 1,
        "connection_id": "cid1",
        "owner_user_id": 1,
        "peer_base_url": "https://peer.example",
        "peer_username": "alice",
        "display_name": "云端",
        "notes": None,
        "icon_name": None,
        "credential_blob": b"enc-pass",
        "access_token_blob": b"enc-token",
        "token_expires_at": None,
        "status": "connected",
        "last_error": None,
        "last_seen_at": None,
        "auto_reconnect": True,
        "created_at": 1,
        "updated_at": 1,
    }
    base.update(overrides)
    return BridgeConnectionRow(**base)  # type: ignore[arg-type]


def _mgr() -> BridgeManager:
    return BridgeManager(
        bridge_repo=MagicMock(),
        secret_repo=MagicMock(),
        user_repo=MagicMock(),
        advertise_base_url="http://local.test",
    )


@pytest.mark.asyncio
async def test_update_meta_only_skips_reauth(monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = _mgr()
    row = _row()
    updated = _row(display_name="新名", notes="n1")
    mgr.get_owned = MagicMock(return_value=row)  # type: ignore[method-assign]
    mgr._repo.find_by_display_name = MagicMock(return_value=None)
    mgr._repo.update_settings = MagicMock(return_value=updated)
    login = AsyncMock()
    monkeypatch.setattr("octop.infra.bridge.manager.login_peer", login)

    out = await mgr.update_connection_meta(
        "cid1",
        owner_user_id=1,
        display_name="新名",
        notes="n1",
        update_notes=True,
        peer_base_url="https://peer.example",
        peer_username="alice",
        password="",
    )
    assert out.display_name == "新名"
    login.assert_not_awaited()
    mgr._repo.update_settings.assert_called_once()
    assert mgr._repo.update_settings.call_args.kwargs.get("update_credentials") is not True


@pytest.mark.asyncio
async def test_update_endpoint_reauths_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mgr = _mgr()
    row = _row()
    after = _row(peer_base_url="https://other.example", peer_username="bob")
    mgr.get_owned = MagicMock(return_value=row)  # type: ignore[method-assign]
    mgr._repo.find_by_display_name = MagicMock(return_value=None)
    mgr._repo.update_settings = MagicMock(return_value=after)
    mgr._repo.get = MagicMock(return_value=after)
    mgr.disconnect = AsyncMock()  # type: ignore[method-assign]
    mgr.connect = AsyncMock(return_value=after)  # type: ignore[method-assign]
    login = AsyncMock(return_value={"access_token": "tok", "expires_in": 3600})
    monkeypatch.setattr("octop.infra.bridge.manager.login_peer", login)
    monkeypatch.setattr(
        "octop.infra.bridge.manager.normalize_peer_base_url",
        lambda raw: raw.strip().rstrip("/"),
    )
    monkeypatch.setattr(
        "octop.infra.bridge.manager.encrypt_payload",
        lambda _repo, payload: f"enc:{payload}".encode(),
    )
    monkeypatch.setattr(
        "octop.infra.bridge.manager.decrypt_payload",
        lambda _repo, _blob: {"password": "old-secret"},
    )

    out = await mgr.update_connection_meta(
        "cid1",
        owner_user_id=1,
        display_name="云端",
        peer_base_url="https://other.example",
        peer_username="bob",
        password=None,
    )
    assert out.peer_base_url == "https://other.example"
    login.assert_awaited_once()
    assert login.await_args.kwargs["username"] == "bob"
    assert login.await_args.kwargs["password"] == "old-secret"
    mgr.disconnect.assert_awaited_once_with("cid1")
    mgr.connect.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_password_required_when_no_stored_secret() -> None:
    mgr = _mgr()
    row = _row(credential_blob=None)
    mgr.get_owned = MagicMock(return_value=row)  # type: ignore[method-assign]
    mgr._repo.find_by_display_name = MagicMock(return_value=None)
    with pytest.raises(OctopError) as ei:
        await mgr.update_connection_meta(
            "cid1",
            owner_user_id=1,
            peer_base_url="https://other.example",
            password="",
        )
    assert ei.value.code == ErrorCode.BRIDGE_AUTH_FAILED
