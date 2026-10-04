"""Strata control plane and installed-package metadata."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("strata-compute-engine")
except PackageNotFoundError:
    __version__ = "0+uninstalled"
