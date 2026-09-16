"""Support ``python -m tripml`` in local automation and containers."""

from tripml.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
