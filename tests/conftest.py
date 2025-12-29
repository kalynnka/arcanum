from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from arcanum.association import Relation, RelationCollection
from arcanum.database import AsyncSession
from tests.models import Base
from tests.schemas import (
    Author,
    Book,
    BookDetail,
    Category,
    Publisher,
    Review,
    Translator,
    sqlalchemy_materia,
)

# Database URL points to the docker-compose postgres service exposed on localhost
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/arcanum"
ASYNC_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/arcanum"


@pytest_asyncio.fixture(scope="session")
async def async_engine():
    engine = create_async_engine(ASYNC_DB_URL, echo=True, future=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def engine(async_engine: AsyncEngine):
    sync_engine = create_engine(DB_URL, echo=True, future=True)

    try:
        yield sync_engine
    finally:
        sync_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def materia():
    with sqlalchemy_materia:
        yield


@pytest_asyncio.fixture(scope="session")
async def publisher(async_engine: AsyncEngine) -> Publisher:
    """Persist and return a Publisher without books."""
    async with AsyncSession(async_engine) as session:
        pub = Publisher(name="Cambridge University Press", country="United Kingdom")
        session.add(pub)
        await session.flush()
        await session.commit()
        await session.refresh(pub)
        return pub


@pytest_asyncio.fixture(scope="session")
async def author(async_engine: AsyncEngine) -> Author:
    """Persist and return an Author without books."""
    async with AsyncSession(async_engine) as session:
        auth = Author(name="Stephen Hawking", field="Physics")
        session.add(auth)
        await session.flush()
        await session.commit()
        await session.refresh(auth)
        return auth


@pytest_asyncio.fixture(scope="session")
async def category(async_engine: AsyncEngine) -> Category:
    """Persist and return a Category."""
    async with AsyncSession(async_engine) as session:
        cat = Category(
            name="Cosmology",
            description="Study of the origin and evolution of the universe",
        )
        session.add(cat)
        await session.flush()
        await session.commit()
        await session.refresh(cat)
        return cat


@pytest_asyncio.fixture(scope="session")
async def book_with_relations(async_engine: AsyncEngine) -> Book:
    """Persist and return a Book with all relationships (1-1, M-1, M-M)."""
    async with AsyncSession(async_engine) as session:
        # Create Book with M-1 relationships
        book = Book(
            title="A Brief History of Time",
            year=1988,
            author=Relation(Author(name="Stephen Hawking", field="Physics")),
            publisher=Relation(Publisher(name="Bantam Books", country="United States")),
            categories=RelationCollection(
                [
                    Category(
                        name="Theoretical Physics",
                        description="Study of physical theories",
                    ),
                    Category(
                        name="Popular Science",
                        description="Science for general audience",
                    ),
                ]
            ),
            detail=Relation(
                BookDetail(
                    isbn="978-0553380163",
                    pages=212,
                    abstract="A landmark volume in science writing by one of the great minds of our time.",
                ),
            ),
        )
        session.add(book)
        await session.flush()
        await session.commit()
        await session.refresh(book)
        return book


@pytest_asyncio.fixture(scope="session")
async def author_with_books(async_engine: AsyncEngine) -> Author:
    """Persist and return an Author with multiple Books (one-to-many)."""
    async with AsyncSession(async_engine) as session:
        pub = Publisher(name="W. W. Norton", country="United States")
        session.add(pub)

        auth = Author(name="Richard Feynman", field="Quantum Physics")
        session.add(auth)
        await session.flush()

        book1 = Book(
            title="The Feynman Lectures on Physics",
            year=1964,
            author=Relation(auth),
            publisher=Relation(pub),
        )
        book2 = Book(
            title="QED: The Strange Theory of Light and Matter",
            year=1985,
            author=Relation(auth),
            publisher=Relation(pub),
        )
        session.add(book1)
        session.add(book2)
        await session.flush()
        await session.commit()
        await session.refresh(auth)
        return auth


@pytest_asyncio.fixture()
async def book_with_translator(async_engine: AsyncEngine) -> Book:
    """Persist and return a Book with an optional Translator (1-1 optional relationship)."""
    async with AsyncSession(async_engine) as session:
        pub = Publisher(name="Springer", country="Germany")
        session.add(pub)

        auth = Author(name="Albert Einstein", field="Physics")
        session.add(auth)

        # Create a Translator (optional 1-1)
        translator = Translator(name="Robert W. Lawson", language="English")
        session.add(translator)
        await session.flush()

        # Create a translated book
        book = Book(
            title="Relativity: The Special and General Theory",
            year=1920,
            author=Relation(auth),
            publisher=Relation(pub),
            translator=Relation(translator),  # Optional 1-1 relationship
        )
        session.add(book)
        await session.flush()
        await session.commit()
        await session.refresh(book)
        return book


@pytest_asyncio.fixture()
async def book_with_reviews(async_engine: AsyncEngine) -> Book:
    """Persist and return a Book with optional Reviews (1-M optional relationship)."""
    async with AsyncSession(async_engine) as session:
        pub = Publisher(name="Vintage Books", country="United States")
        session.add(pub)

        auth = Author(name="James Watson", field="Biology")
        session.add(auth)
        await session.flush()

        book = Book(
            title="The Double Helix",
            year=1968,
            author=Relation(auth),
            publisher=Relation(pub),
        )
        session.add(book)
        await session.flush()

        # Add optional reviews (1-M)
        review1 = Review(
            reviewer_name="Nature Magazine",
            rating=5,
            comment="A groundbreaking account of DNA structure discovery.",
            book=Relation(book),
        )
        review2 = Review(
            reviewer_name="Science Journal",
            rating=4,
            comment="An insightful personal narrative of scientific discovery.",
            book=Relation(book),
        )
        session.add(review1)
        session.add(review2)
        await session.flush()
        await session.commit()
        await session.refresh(book)
        return book
