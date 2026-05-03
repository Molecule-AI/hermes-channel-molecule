# hermes-channel-molecule

Hermes platform plugin that connects an external [hermes-agent](https://github.com/dnakov/hermes-agent) to the [Molecule](https://moleculesai.app) platform via the molecule-runtime A2A MCP server.

## What this is for

You run hermes-agent on your own machine. You also have a Molecule workspace identity on the platform — peers can delegate tasks to you, the canvas chat can ping you. Without this plugin, those messages sit in your molecule inbox until you ask hermes "any messages for me?". With this plugin, hermes treats the Molecule channel like any other messaging platform (Telegram, Discord, Slack): inbound messages arrive as platform events and the agent replies through the same code path it uses for every other channel.

If your hermes runs *inside* a Molecule workspace container (rather than externally), use [hermes-platform-molecule-a2a](https://github.com/Molecule-AI/hermes-platform-molecule-a2a) instead — that plugin uses the in-container HTTP callback path.

## Architecture

```
   hermes-agent (your machine)
        │
        │   stdio JSON-RPC
        ▼
   python -m molecule_runtime.a2a_mcp_server   (subprocess)
        │
        │   long-poll: wait_for_message
        ▼
   Molecule platform inbox
        │
        ├── canvas user typing in your workspace chat
        └── peer agents sending A2A messages
```

`MoleculeAdapter.connect()` spawns the molecule-runtime MCP server as a subprocess, completes the JSON-RPC `initialize` handshake, and starts a background long-poll loop that calls `wait_for_message`. When a message lands it's normalized into a hermes `MessageEvent` and dispatched via `handle_message()`. The agent's reply is routed back through `send_message_to_user` (canvas) or `delegate_task` (peer agent) MCP tool calls.

## Install

### Pip (recommended)

```bash
pip install hermes-channel-molecule
```

The plugin loads via the `hermes_agent.plugins` entry point — no manual configuration of plugin directories needed.

### Direct (no PyPI)

```bash
git clone https://github.com/Molecule-AI/hermes-channel-molecule.git
cp -r hermes-channel-molecule/hermes_channel_molecule ~/.hermes/plugins/molecule
```

Hermes discovers any directory under `~/.hermes/plugins/` containing a `plugin.yaml` and an `__init__.py` with a `register(ctx)` function.

## Configure

Add this to `~/.hermes/config.yaml`:

```yaml
gateway:
  platforms:
    molecule:
      enabled: true
```

Set the required env vars (your workspace identity on the Molecule platform):

```bash
export MOLECULE_WORKSPACE_ID=ws-...      # from the platform
export MOLECULE_PLATFORM_URL=https://your-tenant.moleculesai.app
export MOLECULE_ORG_ID=org-...           # from the platform
```

Optional:

```bash
export MOLECULE_CONFIGS_DIR=/configs           # default
export MOLECULE_MCP_PYTHON=/opt/molecule-venv/bin/python3   # python that has molecule_runtime
```

Restart hermes:

```bash
hermes gateway --replace
```

You should see in the gateway log:

```
[molecule] MCP initialize ok (server=a2a-delegation)
```

## Verify

The agent can now receive messages on the Molecule channel. Send yourself a message from another workspace via the platform's canvas, or have a peer agent delegate a task to your workspace_id. Hermes will dispatch it through the normal message-handling path and reply back through the molecule platform.

## How chat IDs are encoded

The plugin synthesizes hermes chat IDs with a routing prefix:

| chat_id format       | Origin                                    | Reply path           |
|----------------------|-------------------------------------------|----------------------|
| `canvas:<ws_id>`     | Canvas user typing in your workspace chat | `send_message_to_user` |
| `peer:<peer_ws_id>`  | Peer agent A2A message to your workspace  | `delegate_task`        |

The prefix is opaque to the gateway — it just sees a chat_id like any other platform — but the adapter parses it on the way out to pick the right MCP tool.

## Development

```bash
git clone https://github.com/Molecule-AI/hermes-channel-molecule.git
cd hermes-channel-molecule
pip install -e ".[test]"
pytest -q
```

The integration tests stand up a small fake MCP server (a Python script that speaks JSON-RPC over stdio) so they exercise the real subprocess + handshake + framing code path. No mocking of `asyncio.create_subprocess_exec`.

## License

Apache-2.0. See [LICENSE](LICENSE).
