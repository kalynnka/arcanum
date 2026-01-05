"""
Performance test configuration and fixtures.

This module provides shared fixtures for all performance benchmarks including:
- Database setup and teardown
- Data generation fixtures
- Materia configuration
"""

from __future__ import annotations

import uuid
from typing import Generator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.orm import sessionmaker

from tests.models import (
    Author,
    Base,
    Book,
    BookCategory,
    BookDetail,
    Category,
    Publisher,
    Review,
)
from tests.transmuters import sqlalchemy_materia

# Database URL points to the docker-compose postgres service exposed on localhost
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/arcanum"


@pytest.fixture(scope="function")
def test_id() -> uuid.UUID:
    """Generate a unique UUID for test case data isolation."""
    return uuid.uuid4()


@pytest.fixture(scope="session")
def engine() -> Generator[Engine, None, None]:
    """Create a database engine for performance tests."""
    sync_engine = create_engine(
        DB_URL,
        echo=False,  # Disable echo for performance tests
        future=True,
        pool_size=10,
        max_overflow=20,
    )

    try:
        yield sync_engine
    finally:
        sync_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def setup_database(engine: Engine):
    """Set up the database schema."""
    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)


@pytest.fixture
def materia():
    """Activate sqlalchemy_materia for tests that need it."""
    with sqlalchemy_materia:
        yield sqlalchemy_materia


@pytest.fixture(scope="session")
def session_factory(engine: Engine):
    """Create a session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(scope="session")
def arcanum_session_factory(engine: Engine):
    """Create an arcanum session factory."""
    from arcanum.materia.sqlalchemy import Session as ArcanumSession

    return sessionmaker(bind=engine, expire_on_commit=False, class_=ArcanumSession)


@pytest.fixture
def db_session(session_factory) -> Generator[SASession, None, None]:
    """Create a database session for each test."""
    session = session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def arcanum_session(arcanum_session_factory, materia):
    """Create an arcanum session for each test (activates materia automatically)."""
    from arcanum.materia.sqlalchemy import Session as ArcanumSession

    session: ArcanumSession = arcanum_session_factory()
    try:
        with session:
            yield session
    finally:
        session.rollback()
        session.close()


def create_test_data(
    session: SASession,
    num_authors: int = 10,
    books_per_author: int = 5,
    test_id: uuid.UUID | None = None,
) -> list[Author]:
    """
    Create test data for performance benchmarks.

    Creates a hierarchy of:
    - Authors (num_authors)
    - Each author has books (books_per_author)
    - Each book has a publisher, detail, categories, and reviews

    Args:
        session: SQLAlchemy session
        num_authors: Number of authors to create
        books_per_author: Number of books per author
        test_id: UUID for test isolation (used in unique constraints)
    """
    # Generate unique prefix for this test run
    prefix = str(test_id)[:8] if test_id else str(uuid.uuid4())[:8]

    publishers = [
        Publisher(
            name=f"Publisher {prefix}-{i}", country=f"Country {i % 10}", test_id=test_id
        )
        for i in range(5)
    ]
    session.add_all(publishers)
    session.flush()

    categories = [
        Category(
            name=f"Category {prefix}-{i}",
            description=f"Description for category {i}",
            test_id=test_id,
        )
        for i in range(10)
    ]
    session.add_all(categories)
    session.flush()

    authors = []
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]

    for i in range(num_authors):
        author = Author(
            name=f"Author {prefix}-{i}", field=fields[i % len(fields)], test_id=test_id
        )
        session.add(author)
        session.flush()

        for j in range(books_per_author):
            book = Book(
                title=f"Book {prefix}-{i}-{j}",
                year=2000 + (j % 25),
                author=author,
                publisher=publishers[j % len(publishers)],
                test_id=test_id,
            )
            session.add(book)
            session.flush()

            # Add book detail with unique ISBN (max 20 chars)
            detail = BookDetail(
                isbn=f"{prefix[:6]}{i:02d}{j:02d}",
                pages=100 + j * 10,
                abstract=f"Abstract for book {i}-{j}. " * 5,
                book=book,
                test_id=test_id,
            )
            session.add(detail)

            # Add categories (2-3 per book)
            for k in range(2 + j % 2):
                book_cat = BookCategory(
                    book_id=book.id,
                    category_id=categories[(i + j + k) % len(categories)].id,
                    test_id=test_id,
                )
                session.add(book_cat)

            # Add reviews (0-3 per book)
            for k in range(j % 4):
                review = Review(
                    reviewer_name=f"Reviewer {prefix}-{i}-{j}-{k}",
                    rating=1 + (k % 5),
                    comment=f"Comment for review {k}. " * 3,
                    book=book,
                    test_id=test_id,
                )
                session.add(review)

        authors.append(author)

    session.commit()
    return authors


@pytest.fixture(scope="session")
def seeded_database(engine: Engine, session_factory):
    """Seed the database with test data for benchmarks."""
    session = session_factory()
    test_id = uuid.uuid4()
    try:
        # Create test data: 50 authors, 10 books each = 500 books
        authors = create_test_data(
            session, num_authors=50, books_per_author=10, test_id=test_id
        )
        session.commit()
        yield len(authors)
    finally:
        session.close()


@pytest.fixture(scope="session")
def large_seeded_database(engine: Engine, session_factory):
    """Seed the database with larger test data for more intensive benchmarks."""
    session = session_factory()
    test_id = uuid.uuid4()
    try:
        # Create test data: 100 authors, 20 books each = 2000 books
        authors = create_test_data(
            session, num_authors=100, books_per_author=20, test_id=test_id
        )
        session.commit()
        yield len(authors)
    finally:
        session.close()
