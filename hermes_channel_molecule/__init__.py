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
    """Plugin entry point — called by hermes_cli.plugins on discovery."""
    ctx.register_platform_adapter(
        name="molecule",
        adapter_class=MoleculeAdapter,
        requirements_check=check_molecule_requirements,
    )
