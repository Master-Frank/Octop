"""Unit tests for hub-local Bridge shadow history-migration responses."""

from __future__ import annotations

import json

import pytest

from octop.api.middleware.bridge_proxy import _local_bridge_shadow_response
from octop.infra.errors import ErrorCode, OctopError


def test_history_migration_status_is_idle() -> None:
    resp = _local_bridge_shadow_response(
        method="GET",
        rest="/history-migration/status",
    )
    assert resp is not None
    body = json.loads(resp.body)
    assert body["remaining"] == 0
    assert body["can_start"] is False


def test_history_migration_start_rejected() -> None:
    with pytest.raises(OctopError) as ei:
        _local_bridge_shadow_response(
            method="POST",
            rest="/history-migration/start",
        )
    assert ei.value.code == ErrorCode.BRIDGE_REMOTE_UNSUPPORTED


def test_status_is_not_short_circuited() -> None:
    """Runtime status must tunnel to the peer (allowlisted), not be faked here."""
    assert (
        _local_bridge_shadow_response(
            method="GET",
            rest="/status",
        )
        is None
    )


def test_other_paths_pass_through() -> None:
    assert (
        _local_bridge_shadow_response(
            method="GET",
            rest="/threads",
        )
        is None
    )
