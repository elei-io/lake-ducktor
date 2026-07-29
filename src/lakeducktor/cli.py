"""LakeDucktor command-line entry point."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from lakeducktor import __version__
from lakeducktor.capabilities import CapabilitiesError, adapter_for
from lakeducktor.config import ConfigurationError, MetadataConfiguration, load_env_file
from lakeducktor.inventory import InventoryError, inventory_catalog
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
    actions.add_parser(
        "inventory",
        help="collect a read-only inventory of current physical lake state",
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
        _LOGGER.error("startup_failed error=%s", error)
        return 1

    _LOGGER.info("detected metadata_backend=%s", detection.backend.value)
    _LOGGER.info("detected lakes=%s", len(detection.metadata_schemas))
    _LOGGER.info(
        "selected adapter=%s",
        type(adapter).__name__,
    )
    if arguments.command == "inventory":
        try:
            inventory = inventory_catalog(configuration, detection)
        except InventoryError as error:
            _LOGGER.error("inventory_failed error=%s", error)
            return 1
        for lake in inventory.lakes:
            _LOGGER.info(
                "detected lake=%s snapshot=%s tables=%s "
                "active_data_files=%s active_data_bytes=%s "
                "active_delete_files=%s active_delete_bytes=%s "
                "dangling_delete_files=%s scheduled_files=%s",
                lake.metadata_schema,
                lake.latest_snapshot_id
                if lake.latest_snapshot_id is not None
                else "none",
                lake.table_count,
                lake.active_data_files,
                lake.active_data_bytes,
                lake.active_delete_files,
                lake.active_delete_bytes,
                lake.dangling_delete_files,
                lake.scheduled_files,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
