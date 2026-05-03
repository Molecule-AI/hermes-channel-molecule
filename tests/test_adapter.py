"""Tests for the molecule-a2a hermes adapter.

These tests stand up a fake MCP server (a tiny Python script that
speaks JSON-RPC over stdio) so the adapter exercises its real
subprocess+pipe code path. Mocking the subprocess directly would
mask the bug-class this adapter is most likely to hit (JSON framing,
demux, handshake order).

Run via: ``pytest -q tests/test_adapter.py`` from the plugin root.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

# Resolve the adapter file relative to the package, regardless of where
# pytest is invoked from. The tests stub gateway.platforms.base before
# loading adapter.py, so we sidestep `import hermes_channel_molecule`
# (which would trigger the real base-class import) and load via importlib.
import importlib.util

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ADAPTER_PATH = _REPO_ROOT / "hermes_channel_molecule" / "adapter.py"


def _load_adapter_module():
    """Import adapter.py without going through hermes_cli.plugins.

    The real plugin loader supplies hermes_cli.plugins; in tests we stub
    the only symbol the adapter needs (PluginPlatformIdentifier) so the
    import doesn't pull the whole hermes-agent tree.
    """
    fake_plugins = type(sys)("hermes_cli.plugins")

    class PluginPlatformIdentifier:
        __slots__ = ("value",)

        def __init__(self, name: str) -> None:
            self.value = name

        def __hash__(self) -> int:
            return hash(("__plugin_platform__", self.value))

        def __eq__(self, other: object) -> bool:
            return (
                isinstance(other, PluginPlatformIdentifier)
                and self.value == other.value
            )

    fake_plugins.PluginPlatformIdentifier = PluginPlatformIdentifier
    sys.modules.setdefault("hermes_cli", type(sys)("hermes_cli"))
    sys.modules["hermes_cli.plugins"] = fake_plugins

    # Stub gateway.platforms.base — the adapter only uses
    # BasePlatformAdapter, MessageEvent, MessageType, SendResult.
    fake_gateway = type(sys)("gateway")
    fake_platforms = type(sys)("gateway.platforms")
    fake_base = type(sys)("gateway.platforms.base")

    from dataclasses import dataclass, field
    from enum import Enum
    from typing import Optional, List, Dict, Any as TAny
    from datetime import datetime

    class MessageType(Enum):
        TEXT = "text"

    @dataclass
    class SendResult:
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None
        raw_response: TAny = None
        retryable: bool = False

    @dataclass
    class MessageEvent:
        text: str
        message_type: TAny = MessageType.TEXT
        source: TAny = None
        raw_message: TAny = None
        message_id: Optional[str] = None
        media_urls: List[str] = field(default_factory=list)
        media_types: List[str] = field(default_factory=list)
        reply_to_message_id: Optional[str] = None
        reply_to_text: Optional[str] = None
        timestamp: TAny = field(default_factory=datetime.now)

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform
            self._running = False
            self.handled: list = []
            self._fatal_error_message = None
            self._fatal_error_code = None
            self._fatal_error_retryable = True

        def _mark_connected(self) -> None:
            self._running = True

        def _mark_disconnected(self) -> None:
            self._running = False

        @property
        def is_connected(self) -> bool:
            return self._running

        def _set_fatal_error(self, code, message, *, retryable):
            self._fatal_error_code = code
            self._fatal_error_message = message
            self._fatal_error_retryable = retryable

        def build_source(self, **kw):
            return kw

        async def handle_message(self, event):
            self.handled.append(event)

    fake_base.BasePlatformAdapter = BasePlatformAdapter
    fake_base.MessageEvent = MessageEvent
    fake_base.MessageType = MessageType
    fake_base.SendResult = SendResult
    fake_platforms.base = fake_base
    fake_gateway.platforms = fake_platforms
    sys.modules["gateway"] = fake_gateway
    sys.modules["gateway.platforms"] = fake_platforms
    sys.modules["gateway.platforms.base"] = fake_base

    spec = importlib.util.spec_from_file_location("molecule_a2a_adapter", _ADAPTER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


adapter_mod = _load_adapter_module()


# ----------------------------------------------------------------------
# Fake MCP server — a tiny script the adapter spawns instead of the real
# molecule_runtime.a2a_mcp_server. Behavior is driven by env vars so each
# test can configure responses without rewriting the script.
# ----------------------------------------------------------------------

_FAKE_MCP_SERVER = textwrap.dedent("""
    import json, os, sys, time

    # Optional: emit a fixed sequence of JSON responses for tools/call by
    # name, encoded as MCP `content` text. Configure via env:
    #   FAKE_TOOL_<NAME> = JSON-string returned as the tool's text payload
    #   FAKE_TOOL_<NAME>_RAW = if set, return as-is (no extra wrapping)
    # wait_for_message gets special handling: if FAKE_INBOX is set to a
    # newline-delimited list of JSON dicts, each call pops the next one;
    # exhaustion returns the timeout sentinel.

    inbox_iter = iter(
        [json.loads(line) for line in os.environ.get("FAKE_INBOX", "").splitlines() if line.strip()]
    )

    # If FAKE_ENV_DUMP is set, dump the env once at startup so tests
    # can assert what was actually passed through to the subprocess.
    dump_path = os.environ.get("FAKE_ENV_DUMP", "")
    if dump_path:
        with open(dump_path, "w") as f:
            json.dump(dict(os.environ), f)

    def respond(req_id, result):
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\\n")
        sys.stdout.flush()

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        msg = json.loads(raw)
        method = msg.get("method", "")
        req_id = msg.get("id")
        params = msg.get("params", {}) or {}

        if method == "initialize":
            respond(req_id, {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fake-a2a", "version": "0.0.1"},
            })
        elif method == "notifications/initialized":
            pass
        elif method == "tools/call":
            name = params.get("name", "")
            if name == "wait_for_message":
                try:
                    payload = next(inbox_iter)
                except StopIteration:
                    payload = {"timeout": True, "timeout_secs": 0}
                respond(req_id, {"content": [{"type": "text", "text": json.dumps(payload)}]})
            else:
                key = f"FAKE_TOOL_{name.upper()}"
                if key in os.environ:
                    text = os.environ[key]
                else:
                    text = json.dumps({"ok": True, "tool": name, "args": params.get("arguments", {})})
                respond(req_id, {"content": [{"type": "text", "text": text}]})
        else:
            if req_id is not None:
                respond(req_id, {})
""")


@pytest.fixture
def fake_mcp_script(tmp_path):
    script = tmp_path / "fake_mcp.py"
    script.write_text(_FAKE_MCP_SERVER)
    return script


def _make_adapter(monkeypatch, fake_mcp_script: Path, *, env_extra: dict | None = None):
    """Build a MoleculeAdapter that spawns the fake MCP script."""
    # Force _resolve_python to return THIS python — guarantees the fake
    # script is interpretable in the test environment.
    monkeypatch.setattr(adapter_mod, "_resolve_python", lambda: sys.executable)

    monkeypatch.setenv("MOLECULE_WORKSPACE_ID", "ws-test-1234")
    monkeypatch.setenv("MOLECULE_PLATFORM_URL", "http://platform:8080")
    monkeypatch.setenv("MOLECULE_ORG_ID", "org-test")
    if env_extra:
        for k, v in env_extra.items():
            monkeypatch.setenv(k, v)

    cfg = type("Cfg", (), {"extra": {}, "enabled": True})()
    a = adapter_mod.MoleculeAdapter(cfg)

    # Override the spawn target to point at our fake script instead of
    # the `-m molecule_runtime.a2a_mcp_server` module.
    real_create_subprocess_exec = asyncio.create_subprocess_exec

    async def fake_spawn(*args, **kwargs):
        # Replace the trailing `-m molecule_runtime.a2a_mcp_server` args
        # with the path to the fake script.
        new_args = (args[0], str(fake_mcp_script))
        return await real_create_subprocess_exec(*new_args, **kwargs)

    monkeypatch.setattr(adapter_mod.asyncio, "create_subprocess_exec", fake_spawn)
    return a


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_completes_handshake_and_starts_loops(monkeypatch, fake_mcp_script):
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        ok = await a.connect()
        assert ok is True
        assert a.is_connected is True
        assert a._poll_task is not None
        assert a._reader_task is not None
        assert a._fatal_error_message is None
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_subprocess_env_includes_workspace_token(monkeypatch, fake_mcp_script, tmp_path):
    """Regression: MOLECULE_WORKSPACE_TOKEN must reach the spawned MCP
    subprocess. molecule_runtime.platform_auth.get_token reads it for
    external runtimes (no /configs/.auth_token file). Without this
    passthrough the runtime sends every outbound platform call
    unauthenticated and the platform 401s.
    """
    env_dump = tmp_path / "env-dump.json"
    a = _make_adapter(
        monkeypatch,
        fake_mcp_script,
        env_extra={
            "MOLECULE_WORKSPACE_TOKEN": "tok-abc-XYZ-123",
            "FAKE_ENV_DUMP": str(env_dump),
        },
    )
    try:
        assert await a.connect()
        assert env_dump.exists(), "fake server should have dumped env"
        captured = json.loads(env_dump.read_text())
        # Exact-equality on the token. Substring-in-repr would pass
        # whether the var was passed through OR whether something else
        # leaked the literal text.
        assert captured.get("MOLECULE_WORKSPACE_TOKEN") == "tok-abc-XYZ-123"
        # And confirm the canonical runtime env vars are also present.
        assert captured.get("WORKSPACE_ID") == "ws-test-1234"
        assert captured.get("PLATFORM_URL") == "http://platform:8080"
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_subprocess_env_omits_workspace_token_when_unset(monkeypatch, fake_mcp_script, tmp_path):
    """When MOLECULE_WORKSPACE_TOKEN isn't set in hermes's env we don't
    fabricate one for the subprocess — the runtime then falls back to
    /configs/.auth_token (in-container path) or sends headers without
    Authorization (which the platform handles as anonymous heartbeat).
    """
    monkeypatch.delenv("MOLECULE_WORKSPACE_TOKEN", raising=False)
    env_dump = tmp_path / "env-dump.json"
    a = _make_adapter(
        monkeypatch,
        fake_mcp_script,
        env_extra={"FAKE_ENV_DUMP": str(env_dump)},
    )
    try:
        assert await a.connect()
        captured = json.loads(env_dump.read_text())
        assert "MOLECULE_WORKSPACE_TOKEN" not in captured
    finally:
        await a.disconnect()
    assert a.is_connected is False


@pytest.mark.asyncio
async def test_send_canvas_routes_to_send_message_to_user(monkeypatch, fake_mcp_script):
    monkeypatch.setenv("FAKE_TOOL_SEND_MESSAGE_TO_USER", json.dumps({"sent": True, "via": "canvas"}))
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        assert await a.connect()
        result = await a.send("canvas:ws-test-1234", "hello canvas")
        assert result.success is True
        # Exact-shape assertion — the fake echoes back what tool was hit.
        assert result.raw_response == {"sent": True, "via": "canvas"}
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_send_peer_routes_to_delegate_task(monkeypatch, fake_mcp_script):
    monkeypatch.setenv(
        "FAKE_TOOL_DELEGATE_TASK",
        json.dumps({"delegated_to": "peer-uuid-7777", "echo_task": "do the thing"}),
    )
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        assert await a.connect()
        result = await a.send("peer:peer-uuid-7777", "do the thing")
        assert result.success is True
        assert result.raw_response == {
            "delegated_to": "peer-uuid-7777",
            "echo_task": "do the thing",
        }
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_send_unknown_chat_id_prefix_fails(monkeypatch, fake_mcp_script):
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        assert await a.connect()
        result = await a.send("telegram:123", "nope")
        assert result.success is False
        assert "unknown chat_id prefix" in (result.error or "")
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_long_poll_dispatches_canvas_message_and_acks(monkeypatch, fake_mcp_script):
    inbox_msg = {
        "activity_id": "act-aaa-111",
        "text": "hi from canvas",
        "peer_id": "",
        "kind": "canvas_user",
        "method": "message/send",
        "created_at": "2026-05-03T00:00:00Z",
    }
    monkeypatch.setenv("FAKE_INBOX", json.dumps(inbox_msg))
    monkeypatch.setenv(
        "FAKE_TOOL_INBOX_POP",
        json.dumps({"acked": "act-aaa-111"}),
    )
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        assert await a.connect()
        # Wait briefly for the long-poll loop to see the message.
        for _ in range(50):
            if a.handled:
                break
            await asyncio.sleep(0.05)
        assert len(a.handled) == 1, "expected exactly one MessageEvent dispatched"
        ev = a.handled[0]
        # Exact-equality on what the gateway sees.
        assert ev.text == "hi from canvas"
        assert ev.message_id == "act-aaa-111"
        assert ev.source["chat_id"] == "canvas:ws-test-1234"
        assert ev.source["user_id"] == "canvas"
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_long_poll_dispatches_peer_message(monkeypatch, fake_mcp_script):
    inbox_msg = {
        "activity_id": "act-bbb-222",
        "text": "ping from peer",
        "peer_id": "ws-peer-9999",
        "kind": "peer_agent",
        "method": "tasks/send",
        "created_at": "2026-05-03T00:00:01Z",
    }
    monkeypatch.setenv("FAKE_INBOX", json.dumps(inbox_msg))
    a = _make_adapter(monkeypatch, fake_mcp_script)
    try:
        assert await a.connect()
        for _ in range(50):
            if a.handled:
                break
            await asyncio.sleep(0.05)
        assert len(a.handled) == 1
        ev = a.handled[0]
        assert ev.text == "ping from peer"
        # Peer messages encode peer_id into chat_id so the reply path
        # can route back to delegate_task with the right workspace_id.
        assert ev.source["chat_id"] == "peer:ws-peer-9999"
        assert ev.source["user_id"] == "ws-peer-9999"
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_disconnect_terminates_subprocess(monkeypatch, fake_mcp_script):
    a = _make_adapter(monkeypatch, fake_mcp_script)
    assert await a.connect()
    proc = a._proc
    assert proc is not None and proc.returncode is None
    await a.disconnect()
    # Either reaped (returncode set) or already collected (proc=None).
    if a._proc is None:
        # Fully torn down — that's the success shape.
        return
    assert a._proc.returncode is not None


def test_decode_chat_id_canvas():
    assert adapter_mod.MoleculeAdapter._decode_chat_id("canvas:ws-1") == ("canvas", "ws-1")


def test_decode_chat_id_peer():
    assert adapter_mod.MoleculeAdapter._decode_chat_id("peer:peer-uuid") == ("peer", "peer-uuid")


def test_decode_chat_id_unknown_prefix():
    # Unknown but well-formed: prefix returned as kind, rest as target.
    assert adapter_mod.MoleculeAdapter._decode_chat_id("telegram:123") == ("telegram", "123")


def test_decode_chat_id_no_prefix():
    assert adapter_mod.MoleculeAdapter._decode_chat_id("bare") == ("", "bare")


def test_extract_inbox_message_timeout_returns_none():
    assert adapter_mod.MoleculeAdapter._extract_inbox_message(
        {"timeout": True, "timeout_secs": 20}
    ) is None


def test_extract_inbox_message_real_message_returns_dict():
    msg = {"activity_id": "x", "text": "hi", "peer_id": "", "kind": "canvas_user"}
    assert adapter_mod.MoleculeAdapter._extract_inbox_message(msg) == msg


def test_extract_inbox_message_garbage_returns_none():
    assert adapter_mod.MoleculeAdapter._extract_inbox_message("not a dict") is None
    assert adapter_mod.MoleculeAdapter._extract_inbox_message({}) is None


def test_extract_tool_payload_parses_json_text():
    raw = {"content": [{"type": "text", "text": '{"k": "v"}'}]}
    assert adapter_mod.MoleculeAdapter._extract_tool_payload(raw) == {"k": "v"}


def test_extract_tool_payload_falls_back_to_text():
    raw = {"content": [{"type": "text", "text": "ok"}]}
    assert adapter_mod.MoleculeAdapter._extract_tool_payload(raw) == "ok"


def test_check_molecule_requirements_missing_workspace_id(monkeypatch):
    monkeypatch.delenv("MOLECULE_WORKSPACE_ID", raising=False)
    assert adapter_mod.check_molecule_requirements() is False
