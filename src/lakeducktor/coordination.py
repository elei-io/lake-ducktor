"""Coordinate recoverable maintenance ownership across workers."""

from __future__ import annotations

from collections.abc import Callable
from hashlib import blake2b
from typing import Protocol

from lakeducktor.config import MetadataConfiguration


class CoordinationError(RuntimeError):
    """Treatment ownership could not be established or released."""


class _QueryResult(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...


class _Connection(Protocol):
    def execute(
        self,
        query: str,
        parameters: tuple[object, ...],
    ) -> _QueryResult: ...

    def close(self) -> None: ...


class TreatmentClaim(Protocol):
    def release(self) -> None:
        """Release ownership. Calling this more than once is harmless."""


class TreatmentCoordinator(Protocol):
    def try_claim(
        self,
        metadata_schema: str,
    ) -> TreatmentClaim | None:
        """Return recoverable lake ownership, or None when another worker owns it."""


type ConnectionFactory = Callable[[MetadataConfiguration], _Connection]


def advisory_lock_key(metadata_schema: str) -> int:
    """Return a stable signed PostgreSQL advisory-lock key for one lake."""

    identity = f"lakeducktor\0{metadata_schema}".encode()
    return int.from_bytes(blake2b(identity, digest_size=8).digest(), signed=True)


def _connect_postgres(configuration: MetadataConfiguration) -> _Connection:
    try:
        import psycopg
    except ImportError as error:
        raise CoordinationError(
            "PostgreSQL coordination requires the optional postgres dependency; "
            "run `uv sync --extra postgres`"
        ) from error
    return psycopg.connect(
        configuration.postgres_uri(),
        autocommit=True,
        application_name="lakeducktor",
    )


class PostgresTreatmentClaim:
    """A session-level advisory lock released by close or process death."""

    def __init__(self, connection: _Connection, lock_key: int) -> None:
        self._connection = connection
        self._lock_key = lock_key
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        release_error: Exception | None = None
        try:
            row = self._connection.execute(
                "SELECT pg_advisory_unlock(%s)",
                (self._lock_key,),
            ).fetchone()
            if row is None or not bool(row[0]):
                release_error = CoordinationError(
                    "PostgreSQL reported that the treatment claim was not held"
                )
        except Exception as error:
            release_error = error
        try:
            self._connection.close()
        except Exception as error:
            if release_error is None:
                release_error = error
        if release_error is not None:
            if isinstance(release_error, CoordinationError):
                raise release_error
            raise CoordinationError(
                "could not release treatment claim"
            ) from release_error


class PostgresTreatmentCoordinator:
    """Acquire non-blocking distributed claims in the metadata database."""

    def __init__(
        self,
        configuration: MetadataConfiguration,
        connect: ConnectionFactory = _connect_postgres,
    ) -> None:
        self._configuration = configuration
        self._connect = connect

    def try_claim(
        self,
        metadata_schema: str,
    ) -> PostgresTreatmentClaim | None:
        lock_key = advisory_lock_key(metadata_schema)
        connection: _Connection | None = None
        try:
            connection = self._connect(self._configuration)
            row = connection.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (lock_key,),
            ).fetchone()
            if row is None:
                raise CoordinationError(
                    "PostgreSQL returned no claim acquisition result"
                )
            if not bool(row[0]):
                connection.close()
                return None
            return PostgresTreatmentClaim(connection, lock_key)
        except CoordinationError:
            if connection is not None:
                connection.close()
            raise
        except Exception as error:
            if connection is not None:
                connection.close()
            raise CoordinationError("could not acquire treatment claim") from error
