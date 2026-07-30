from lakeducktor.config import MetadataConfiguration
from lakeducktor.coordination import (
    PostgresTreatmentCoordinator,
    advisory_lock_key,
)


class FakeResult:
    def __init__(self, value: bool) -> None:
        self.value = value

    def fetchone(self) -> tuple[object, ...]:
        return (self.value,)


class FakeConnection:
    def __init__(self, results: list[bool]) -> None:
        self.results = results
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    def execute(
        self,
        query: str,
        parameters: tuple[object, ...],
    ) -> FakeResult:
        self.queries.append((query, parameters))
        return FakeResult(self.results.pop(0))

    def close(self) -> None:
        self.closed = True


_CONFIGURATION = MetadataConfiguration(
    backend_hint="postgres",
    host="catalog.example",
    port=5432,
    username="user",
    password="password",
    database="lake",
)


def test_advisory_lock_key_is_stable_and_lake_scoped() -> None:
    assert advisory_lock_key("lake") == advisory_lock_key("lake")
    assert advisory_lock_key("lake") != advisory_lock_key("other")


def test_postgres_claim_uses_session_lock_and_releases_idempotently() -> None:
    connection = FakeConnection([True, True])
    coordinator = PostgresTreatmentCoordinator(
        _CONFIGURATION,
        connect=lambda _configuration: connection,
    )

    claim = coordinator.try_claim("lake")

    assert claim is not None
    lock_key = advisory_lock_key("lake")
    assert connection.queries[0] == (
        "SELECT pg_try_advisory_lock(%s)",
        (lock_key,),
    )

    claim.release()
    claim.release()

    assert connection.queries[1] == (
        "SELECT pg_advisory_unlock(%s)",
        (lock_key,),
    )
    assert len(connection.queries) == 2
    assert connection.closed is True


def test_busy_postgres_claim_closes_its_session() -> None:
    connection = FakeConnection([False])
    coordinator = PostgresTreatmentCoordinator(
        _CONFIGURATION,
        connect=lambda _configuration: connection,
    )

    claim = coordinator.try_claim("lake")

    assert claim is None
    assert connection.closed is True
