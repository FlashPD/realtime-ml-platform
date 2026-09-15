"""Shared package for the real-time ML platform."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("tripml")
except PackageNotFoundError:  # pragma: no cover - only possible outside an installed checkout
    __version__ = "0.0.0"

__all__ = ["__version__"]
