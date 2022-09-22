"""Manage the different backends."""

from . import backends
from .backend_interface import Backend


def get_backend(backend_name: str) -> Backend:
    """Get the `Backend` instance per the given name."""
    try:
        return getattr(backends, backend_name).Backend  # type: ignore[no-any-return]
        # TODO - we probably need to except ImportError
    except AttributeError as e:
        raise RuntimeError(f"Unknown backend: {backend_name}") from e
