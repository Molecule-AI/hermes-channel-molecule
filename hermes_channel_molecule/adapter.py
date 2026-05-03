"""molecule-a2a hermes platform adapter.

Connects hermes to the molecule platform via the molecule-runtime A2A MCP
server. The MCP server is spawned as a subprocess on ``connect()``; we
talk to it over stdio JSON-RPC.

Inbound flow
------------
A background long-poll task calls ``wait_for_message`` on the MCP server.
When a real message lands (canvas user typing in the molecule canvas, or
a peer workspace delegating to us), we build a hermes ``MessageEvent``
and dispatch via ``self.handle_message(event)``. After the gateway hands
back a reply (or decides not to), we ack with ``inbox_pop(activity_id)``.

Outbound flow
-------------
``send(chat_id, content)`` parses the prefix:
- ``canvas:<workspace_id>`` → ``send_message_to_user(message=content)``
- ``peer:<peer_workspace_id>`` → ``delegate_task(workspace_id=<peer>, task=content)``

The chat_id encoding is what we hand to hermes inside the
``MessageEvent.source`` so the gateway's session bookkeeping has a stable
key per canvas/peer; the adapter is the only thing that decodes it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from hermes_cli.plugins import PluginPlatformIdentifier

logger = logging.getLogger(__name__)

PLUGIN_NAME = "molecule"

# How long the long-poll task asks the MCP server to block per turn.
# 20s keeps the inbound latency floor tight while spending almost no CPU
# (one JSON-RPC roundtrip every ~20s when idle). The MCP server caps
# wait_for_message at 300s anyway, so picking lower than that is just a
# trade-off between wakeups-per-minute and inbound latency.
_LONG_POLL_SECS = 20


def _resolve_python() -> str:
    """Return the python interpreter that can ``import molecule_runtime``.

    Order of preference matches the codex/hermes templates:
    1. ``MOLECULE_MCP_PYTHON`` env override
    2. ``/opt/molecule-venv/bin/python3`` (the workspace runtime venv —
       only present on platform-managed EC2 workspaces)
    3. ``python3`` / ``python`` from PATH

    Returns the first candidate that ``-c "import molecule_runtime"``
    succeeds against; falls back to the last candidate as a last resort
    so the failure surface is a subprocess error (visible) rather than
    a silent no-op (invisible).
    """
    override = os.environ.get("MOLECULE_MCP_PYTHON", "").strip()
    if override and os.access(override, os.X_OK):
        return override

    candidates = [
        "/opt/molecule-venv/bin/python3",
        "/opt/molecule-venv/bin/python",
        shutil.which("python3") or "",
        shutil.which("python") or "",
    ]
    last_real = ""
    for cand in candidates:
        if not cand or not os.access(cand, os.X_OK):
            continue
        last_real = cand
        try:
            subprocess.run(
                [cand, "-c", "import molecule_runtime"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            return cand
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            continue
    return last_real or "/opt/molecule-venv/bin/python3"


def check_molecule_requirements() -> bool:
    """Return True when the plugin has everything it needs to start.

    Mirrors the ``check_<platform>_requirements()`` pattern in the
    in-tree adapters. The gateway will skip adapter creation (with a
    warning) if this returns False.
    """
    if not os.environ.get("MOLECULE_WORKSPACE_ID"):
        logger.warning(
            "%s: MOLECULE_WORKSPACE_ID is not set; cannot connect", PLUGIN_NAME
        )
        return False
    if not os.environ.get("MOLECULE_WORKSPACE_TOKEN"):
        # Soft-warn rather than block: the runtime will fall back to
        # /configs/.auth_token if present (in-container scenario where
        # this plugin is unusual but not impossible). External runs —
        # the common case — need the env var.
        logger.warning(
            "%s: MOLECULE_WORKSPACE_TOKEN is not set; outbound platform "
            "calls will be unauthenticated unless /configs/.auth_token "
            "exists. Generate a token from the canvas → Tokens tab and "
            "export MOLECULE_WORKSPACE_TOKEN=...", PLUGIN_NAME,
        )
    python = _resolve_python()
    try:
        subprocess.run(
            [python, "-c", "import molecule_runtime"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        logger.warning(
            "%s: %s cannot import molecule_runtime: %s",
            PLUGIN_NAME, python, exc,
        )
        return False
    return True


class MoleculeAdapter(BasePlatformAdapter):
    """Hermes platform adapter for the molecule platform via A2A MCP."""

    def __init__(self, config) -> None:
        super().__init__(config, PluginPlatformIdentifier(PLUGIN_NAME))

        self._workspace_id = os.environ.get("MOLECULE_WORKSPACE_ID", "")
        self._platform_url = os.environ.get(
            "MOLECULE_PLATFORM_URL", "http://platform:8080"
        )
        self._org_id = os.environ.get("MOLECULE_ORG_ID", "")
        self._configs_dir = os.environ.get("MOLECULE_CONFIGS_DIR", "/configs")
        # Per-workspace platform credential. Inside a molecule-managed
        # container the runtime reads this from /configs/.auth_token (the
        # platform writes it on provision); external runtimes (this case)
        # have no /configs volume and must supply it via env. Without it
        # every outbound platform call goes unauthenticated and gets
        # rejected. See molecule_runtime.platform_auth.get_token.
        self._workspace_token = os.environ.get("MOLECULE_WORKSPACE_TOKEN", "")
        self._python = _resolve_python()

        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None

        # JSON-RPC bookkeeping: monotonic id, demux table, write lock.
        self._next_id = 1
        self._pending: Dict[int, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Spawn the MCP server, complete the JSON-RPC handshake, start
        the long-poll loop. Returns True on success.
        """
        env = os.environ.copy()
        # Ensure the subprocess sees the molecule_runtime configuration
        # even when hermes was launched from a shell that doesn't have
        # those vars exported.
        env["WORKSPACE_ID"] = self._workspace_id
        env["PLATFORM_URL"] = self._platform_url
        if self._org_id:
            env["MOLECULE_ORG_ID"] = self._org_id
        if self._configs_dir:
            env["CONFIGS_DIR"] = self._configs_dir
        if self._workspace_token:
            env["MOLECULE_WORKSPACE_TOKEN"] = self._workspace_token

        try:
            self._proc = await asyncio.create_subprocess_exec(
                self._python,
                "-m",
                "molecule_runtime.a2a_mcp_server",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except (OSError, FileNotFoundError) as exc:
            self._set_fatal_error(
                "spawn_failed",
                f"failed to spawn {self._python} -m molecule_runtime.a2a_mcp_server: {exc}",
                retryable=False,
            )
            return False

        self._reader_task = asyncio.create_task(
            self._reader_loop(), name=f"{PLUGIN_NAME}-reader"
        )
        self._stderr_task = asyncio.create_task(
            self._stderr_loop(), name=f"{PLUGIN_NAME}-stderr"
        )

        try:
            init_result = await asyncio.wait_for(
                self._call(
                    "initialize",
                    {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {"name": PLUGIN_NAME, "version": "0.1.0"},
                    },
                ),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            await self.disconnect()
            self._set_fatal_error(
                "init_timeout",
                "MCP initialize handshake timed out after 15s",
                retryable=True,
            )
            return False
        except Exception as exc:  # noqa: BLE001 — surface every init failure
            await self.disconnect()
            self._set_fatal_error(
                "init_failed",
                f"MCP initialize raised: {exc}",
                retryable=True,
            )
            return False

        logger.info(
            "%s: MCP initialize ok (server=%s)",
            PLUGIN_NAME,
            (init_result or {}).get("serverInfo", {}).get("name", "?"),
        )

        # The "initialized" notification is fire-and-forget — no response.
        await self._notify("notifications/initialized", {})

        self._poll_task = asyncio.create_task(
            self._long_poll_loop(), name=f"{PLUGIN_NAME}-poll"
        )

        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Cancel background tasks and tear down the subprocess."""
        for task in (self._poll_task, self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._poll_task, self._reader_task, self._stderr_task):
            if task is None:
                continue
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._poll_task = None
        self._reader_task = None
        self._stderr_task = None

        if self._proc is not None:
            try:
                if self._proc.returncode is None:
                    self._proc.terminate()
                    try:
                        await asyncio.wait_for(self._proc.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        self._proc.kill()
                        await self._proc.wait()
            except ProcessLookupError:
                pass
            self._proc = None

        # Fail any inflight calls so callers don't hang forever.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("MCP subprocess closed"))
        self._pending.clear()

        self._mark_disconnected()

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Route an outbound message based on the chat_id prefix."""
        kind, target = self._decode_chat_id(chat_id)
        try:
            if kind == "canvas":
                payload = await self._call_tool(
                    "send_message_to_user", {"message": content}
                )
            elif kind == "peer":
                payload = await self._call_tool(
                    "delegate_task",
                    {"workspace_id": target, "task": content},
                )
            else:
                return SendResult(
                    success=False,
                    error=f"unknown chat_id prefix: {chat_id!r}",
                )
        except Exception as exc:  # noqa: BLE001 — convert to SendResult
            return SendResult(
                success=False,
                error=str(exc),
                retryable=isinstance(exc, (ConnectionError, asyncio.TimeoutError)),
            )

        return SendResult(success=True, raw_response=payload)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """No-op — the molecule platform doesn't surface typing indicators."""
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        kind, target = self._decode_chat_id(chat_id)
        if kind == "canvas":
            return {"name": "molecule canvas", "type": "dm", "chat_id": chat_id}
        if kind == "peer":
            return {
                "name": f"peer:{target[:8]}",
                "type": "dm",
                "chat_id": chat_id,
            }
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Inbound — long-poll loop
    # ------------------------------------------------------------------

    async def _long_poll_loop(self) -> None:
        """Drain the molecule inbox, dispatching messages to the gateway."""
        while True:
            try:
                raw = await self._call_tool(
                    "wait_for_message", {"timeout_secs": _LONG_POLL_SECS}
                )
            except asyncio.CancelledError:
                raise
            except ConnectionError:
                # Subprocess died — disconnect path will reset state.
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning("%s: wait_for_message raised: %s", PLUGIN_NAME, exc)
                await asyncio.sleep(2.0)
                continue

            message = self._extract_inbox_message(raw)
            if message is None:
                continue  # timeout tick

            try:
                await self._dispatch(message)
            except Exception:  # noqa: BLE001 — never let a bad event kill the loop
                logger.exception("%s: dispatch failed for %r", PLUGIN_NAME, message)

            activity_id = message.get("activity_id", "")
            if activity_id:
                try:
                    await self._call_tool(
                        "inbox_pop", {"activity_id": activity_id}
                    )
                except Exception as exc:  # noqa: BLE001
                    # Worst case: the same message redelivers next poll.
                    # That's a duplicate, not a lost message — log and
                    # continue rather than tear down the loop.
                    logger.warning(
                        "%s: inbox_pop(%s) failed: %s",
                        PLUGIN_NAME, activity_id, exc,
                    )

    async def _dispatch(self, message: Dict[str, Any]) -> None:
        """Build a hermes MessageEvent and hand it to the gateway."""
        kind = message.get("kind", "canvas_user")
        peer_id = message.get("peer_id", "") or ""
        text = message.get("text", "") or ""
        activity_id = message.get("activity_id", "")

        if kind == "peer_agent" and peer_id:
            chat_id = self._encode_chat_id("peer", peer_id)
            chat_name = f"peer:{peer_id[:8]}"
            user_id = peer_id
            user_name = peer_id
        else:
            chat_id = self._encode_chat_id("canvas", self._workspace_id)
            chat_name = "molecule canvas"
            user_id = "canvas"
            user_name = "canvas-user"

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type="dm",
            user_id=user_id,
            user_name=user_name,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=message,
            message_id=activity_id or None,
        )
        await self.handle_message(event)

    # ------------------------------------------------------------------
    # JSON-RPC helpers
    # ------------------------------------------------------------------

    async def _reader_loop(self) -> None:
        """Read stdout lines from the MCP server, demux to pending futures."""
        assert self._proc is not None and self._proc.stdout is not None
        stdout = self._proc.stdout
        while True:
            line = await stdout.readline()
            if not line:
                # Subprocess closed stdout — fail every pending call.
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("MCP stdout closed"))
                self._pending.clear()
                return
            try:
                msg = json.loads(line.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                continue
            req_id = msg.get("id")
            if req_id is None:
                # Notification from the server — ignore for now (push tags
                # would arrive here once we wire the channel notification).
                continue
            fut = self._pending.pop(req_id, None)
            if fut is None or fut.done():
                continue
            if "error" in msg:
                fut.set_exception(RuntimeError(str(msg["error"])))
            else:
                fut.set_result(msg.get("result"))

    async def _stderr_loop(self) -> None:
        """Mirror MCP server stderr into our logger so operators can debug."""
        assert self._proc is not None and self._proc.stderr is not None
        stderr = self._proc.stderr
        while True:
            line = await stderr.readline()
            if not line:
                return
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                logger.info("[mcp-server] %s", text)

    async def _call(self, method: str, params: dict) -> Any:
        """Send a JSON-RPC request and await the response."""
        if self._proc is None or self._proc.stdin is None:
            raise ConnectionError("MCP subprocess not running")
        req_id = self._next_id
        self._next_id += 1
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        ) + "\n"
        async with self._write_lock:
            self._proc.stdin.write(payload.encode("utf-8"))
            await self._proc.stdin.drain()
        return await fut

    async def _notify(self, method: str, params: dict) -> None:
        """Send a JSON-RPC notification (no id, no response)."""
        if self._proc is None or self._proc.stdin is None:
            raise ConnectionError("MCP subprocess not running")
        payload = json.dumps(
            {"jsonrpc": "2.0", "method": method, "params": params}
        ) + "\n"
        async with self._write_lock:
            self._proc.stdin.write(payload.encode("utf-8"))
            await self._proc.stdin.drain()

    async def _call_tool(self, tool_name: str, arguments: dict) -> Any:
        """Convenience wrapper: tools/call → return the parsed result."""
        result = await self._call(
            "tools/call",
            {"name": tool_name, "arguments": arguments},
        )
        return self._extract_tool_payload(result)

    @staticmethod
    def _extract_tool_payload(result: Any) -> Any:
        """Pull the JSON payload out of an MCP tools/call response.

        MCP wraps tool returns as ``{"content": [{"type": "text",
        "text": "..."}]}``. Most molecule tools return a JSON string
        inside that text field; some (commit_memory, send_message_to_user)
        return a plain status string. Try JSON-decode first, fall back
        to the raw text.
        """
        if not isinstance(result, dict):
            return result
        content = result.get("content")
        if not isinstance(content, list) or not content:
            return result
        text = content[0].get("text", "") if isinstance(content[0], dict) else ""
        if not text:
            return result
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return text

    @staticmethod
    def _extract_inbox_message(payload: Any) -> Optional[Dict[str, Any]]:
        """Return the inbox-message dict from a wait_for_message result.

        Returns None for the timeout sentinel ``{"timeout": true, ...}``.
        """
        if not isinstance(payload, dict):
            return None
        if payload.get("timeout"):
            return None
        if "activity_id" not in payload:
            return None
        return payload

    # ------------------------------------------------------------------
    # chat_id helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _encode_chat_id(kind: str, target: str) -> str:
        return f"{kind}:{target}"

    @staticmethod
    def _decode_chat_id(chat_id: str) -> tuple[str, str]:
        if ":" not in chat_id:
            return ("", chat_id)
        kind, _, target = chat_id.partition(":")
        return (kind, target)
