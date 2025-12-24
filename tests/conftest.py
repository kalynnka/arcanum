from __future__ import annotations

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from arcanum.association import Relation
from arcanum.base import validation_context
from tests.models import Base
from tests.schemas import Bar, Foo

# Database URL points to the docker-compose postgres service exposed on localhost
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/arcanum"


@pytest.fixture(scope="session")
def engine():
    """Create a PostgreSQL engine, create all tables at session start and drop them at the end.

    Ensures clean schema for each pytest invocation.
    """
    engine = create_engine(DB_URL, echo=True, future=True)

    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)
        conn.commit()

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def test_session(engine: Engine):
    """Read-only / rollback session for tests that must never commit.

    Keeps prior semantics of the old 'session' fixture: any attempt to commit raises.
    """
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    session = SessionLocal()

    def commit():  # noqa: D401 - simple override
        raise NotImplementedError(
            "Commit is disabled in test_session to ensure isolation."
        )

    session.commit = commit  # type: ignore[assignment]

    with session.begin(), validation_context():
        try:
            yield session
        finally:
            session.rollback()
            session.close()


@pytest.fixture()
def session(engine: Engine):
    """Writable session fixture allowing real commits.

    Use when you need persisted rows visible to subsequent operations within the
    same test. Each test gets a fresh transaction scope; data is not automatically
    rolled back. Prefer 'test_session' unless you explicitly need commits.
    """
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session, validation_context():
        try:
            yield session
        finally:
            # Best effort cleanup: rollback any open transaction then close.
            session.rollback()
            session.close()


@pytest.fixture()
def foo_without_bar(session: Session):
    """Persist and return a Foo without an associated Bar."""
    foo = Foo(name="Foo Without Bar")
    session.add(foo)
    session.flush()  # populate PK
    session.commit()
    session.refresh(foo)
    return foo.__provided__


@pytest.fixture()
def foo_with_bar(session: Session):
    """Persist and return a Foo that has an associated Bar (one-to-many)."""
    bar = Bar(
        data="Bar Data",
        foo=Relation(Foo(name="Bar's Foo0")),
    )
    session.add(bar)
    session.flush()
    session.commit()
    session.refresh(bar)
    return bar.foo.value.__provided__


@pytest.fixture()
def bar_only(session: Session):
    """Create a Foo + Bar but return only the Bar instance (convenience)."""
    bar = Bar(
        data="Isolated Bar",
        foo=Relation(Foo(name="Foo For Bar Only")),
    )
    session.add(bar)
    session.flush()
    session.commit()
    session.refresh(bar)
    return bar.__provided__
