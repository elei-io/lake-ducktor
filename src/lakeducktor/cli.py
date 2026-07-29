"""LakeDucktor command-line entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from lakeducktor import __version__
from lakeducktor.capabilities import CapabilitiesError, adapter_for
from lakeducktor.config import ConfigurationError, MetadataConfiguration, load_env_file
from lakeducktor.lake import BackendDetectionError, detect_metadata_backend

_LOGGER = logging.getLogger("lakeducktor")


def configure_logging() -> None:
    """Configure useful process-level logs without another dependency."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="lakeducktor")
    command.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="environment file to load without overriding existing variables",
    )
    actions = command.add_subparsers(dest="command", required=True)
    actions.add_parser(
        "detect-backend",
        help="attach read-only and report the DuckLake metadata backend",
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    _LOGGER.info("starting version=%s", __version__)
    arguments = parser().parse_args(argv)
    try:
        load_env_file(arguments.env_file)
        configuration = MetadataConfiguration.from_environment()
        detection = detect_metadata_backend(configuration)
        adapter = adapter_for(detection)
    except (CapabilitiesError, ConfigurationError, BackendDetectionError) as error:
        print(f"LakeDucktor: {error}", file=sys.stderr)
        return 1

    if len(detection.metadata_schemas) == 1:
        scope = f"schema={detection.metadata_schemas[0]}"
    else:
        scope = f"schemas={len(detection.metadata_schemas)}"
    _LOGGER.info("detected metadata_backend=%s", detection.backend.value)
    _LOGGER.info("detected %s", scope)
    _LOGGER.info(
        "selected adapter=%s",
        type(adapter).__name__,
    )
    for extension in detection.duckdb_extensions:
        _LOGGER.info(
            "detected extension=%s version=%s",
            extension.name,
            extension.version,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
