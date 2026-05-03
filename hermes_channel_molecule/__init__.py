"""hermes-channel-molecule — hermes platform plugin for the Molecule channel.

Loaded via the ``hermes_agent.plugins`` pip entry-point declared in
``pyproject.toml``, OR via direct install at
``~/.hermes/plugins/<dir>/`` with the bundled ``plugin.yaml`` manifest.
Both load paths claim the platform name ``molecule``.
"""

from .adapter import MoleculeAdapter, check_molecule_requirements

__all__ = [
    "MoleculeAdapter",
    "check_molecule_requirements",
    "register",
]


def register(ctx) -> None:
    """Plugin entry point — called by hermes_cli.plugins on discovery.

    Dual-API shim:
    - Upstream NousResearch/hermes-agent#17751 (merged 2026-04-30)
      shipped ``ctx.register_platform(name, label, adapter_factory,
      check_fn, ...)``. This is the canonical API on stock hermes-agent.
    - Earlier forks (incl. hermes-agent before #17751 landed) expose
      ``ctx.register_platform_adapter(name, adapter_class,
      requirements_check)`` instead — narrower signature, no factory.

    Detect at runtime so the same wheel installs cleanly on both.
    """
    if hasattr(ctx, "register_platform"):
        ctx.register_platform(
            name="molecule",
            label="Molecule",
            adapter_factory=lambda cfg: MoleculeAdapter(cfg),
            check_fn=check_molecule_requirements,
            required_env=["MOLECULE_WORKSPACE_ID", "MOLECULE_PLATFORM_URL"],
            install_hint=(
                "set MOLECULE_WORKSPACE_ID, MOLECULE_WORKSPACE_TOKEN, "
                "MOLECULE_PLATFORM_URL, MOLECULE_ORG_ID; ensure "
                "molecule-ai-workspace-runtime is on the python that "
                "MOLECULE_MCP_PYTHON resolves to"
            ),
        )
    elif hasattr(ctx, "register_platform_adapter"):
        ctx.register_platform_adapter(
            name="molecule",
            adapter_class=MoleculeAdapter,
            requirements_check=check_molecule_requirements,
        )
    else:
        raise RuntimeError(
            "hermes-channel-molecule: this hermes-agent version exposes "
            "neither register_platform (upstream #17751+) nor "
            "register_platform_adapter (legacy fork) — cannot register"
        )
