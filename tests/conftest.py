from __future__ import annotations

import pytest
from sqlalchemy import Engine, create_engine

from arcanum.association import Relation
from arcanum.database import Session
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
def foo_without_bar(engine: Engine) -> Foo:
    """Persist and return a Foo without an associated Bar."""
    with Session(engine) as session:
        foo = Foo(name="Foo Without Bar")
        session.add(foo)
        session.flush()
        session.commit()
        session.refresh(foo)
        return foo


@pytest.fixture()
def foo_with_bar(engine: Engine) -> Foo:
    """Persist and return a Foo that has an associated Bar (one-to-many)."""
    with Session(engine) as session:
        foo = Foo(name="Bar's Foo0")
        bar1 = Bar(
            data="Bar1 Data",
            foo=Relation(foo),
        )
        bar2 = Bar(
            data="Bar2 Data",
            foo=Relation(foo),
        )
        session.add(bar1)
        session.add(bar2)
        session.flush()
        session.commit()
        session.refresh(foo)
        return foo


@pytest.fixture()
def bar_only(engine: Engine) -> Bar:
    """Create a Foo + Bar but return only the Bar instance (convenience)."""
    with Session(engine) as session:
        bar = Bar(
            data="Isolated Bar",
            foo=Relation(Foo(name="Foo For Bar Only")),
        )
        session.add(bar)
        session.flush()
        session.commit()
        session.refresh(bar)
        return bar
