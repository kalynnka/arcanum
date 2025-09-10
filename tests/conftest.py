from __future__ import annotations

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from tests.models import (  # noqa: F401  (metadata_obj may be unused in tests directly)
    Base,
    metadata_obj,
)
from tests.schemas import Bar, Foo  # noqa: F401 - exported for tests

# Database URL points to the docker-compose postgres service exposed on localhost
DB_URL = "postgresql+psycopg2://protocollum:dev_password@localhost:5432/protocollum_dev"


@pytest.fixture(scope="session")
def engine():  # type: ignore[annotations]
    """Create a PostgreSQL engine, create all tables at session start and drop them at the end.

    Ensures clean schema for each pytest invocation.
    """
    engine = create_engine(DB_URL, echo=False, future=True)

    # (Re)create schema objects
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    try:
        yield engine
    finally:  # Always drop tables even if tests failed
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def session(engine: Engine):
    """Provide a database session wrapped in a SAVEPOINT for test isolation.

    Each test runs in a transaction that's rolled back afterwards.
    """
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    session = SessionLocal()

    def commit():
        raise NotImplementedError("Commit is disabled in tests to ensure isolation.")

    session.commit = commit

    with session.begin():
        try:
            yield session
        finally:
            session.rollback()
            session.close()
