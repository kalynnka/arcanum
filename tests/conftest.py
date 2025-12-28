from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from arcanum.association import Relation
from arcanum.database import AsyncSession, Session
from tests.models import Base
from tests.schemas import Bar, Foo

# Database URL points to the docker-compose postgres service exposed on localhost
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/arcanum"
ASYNC_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/arcanum"


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


@pytest_asyncio.fixture(scope="session")
async def async_engine():
    """Create an async PostgreSQL engine using asyncpg.

    Creates all tables at session start and drops them at the end.
    Ensures clean schema for each pytest invocation.
    """
    engine = create_async_engine(ASYNC_DB_URL, echo=True, future=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    try:
        yield engine
    finally:
        await engine.dispose()


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


@pytest_asyncio.fixture()
async def async_foo_without_bar(async_engine: AsyncEngine) -> Foo:
    """Persist and return a Foo without an associated Bar (async)."""
    async with AsyncSession(async_engine, expire_on_commit=False) as session:
        foo = Foo(name="Foo Without Bar")
        session.add(foo)
        await session.flush()
        await session.commit()
        await session.refresh(foo)
        return foo


@pytest_asyncio.fixture()
async def async_foo_with_bar(async_engine: AsyncEngine) -> Foo:
    """Persist and return a Foo that has an associated Bar (one-to-many, async)."""
    async with AsyncSession(async_engine, expire_on_commit=False) as session:
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
        await session.flush()
        await session.commit()
        await session.refresh(foo)
        return foo


@pytest_asyncio.fixture()
async def async_bar_only(async_engine: AsyncEngine) -> Bar:
    """Create a Foo + Bar but return only the Bar instance (convenience, async)."""
    async with AsyncSession(async_engine, expire_on_commit=False) as session:
        bar = Bar(
            data="Isolated Bar",
            foo=Relation(Foo(name="Foo For Bar Only")),
        )
        session.add(bar)
        await session.flush()
        await session.commit()
        await session.refresh(bar)
        return bar
