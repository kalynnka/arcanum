from __future__ import annotations

import random
from typing import Any, Generator

import pytest

from arcanum.materia.sqlalchemy import Session as ArcanumSession
from arcanum.materia.sqlalchemy.base import SqlalchemyMateria
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker
from tests.models import Author as AuthorModel
from tests.models import Base
from tests.models import Book as BookModel
from tests.models import Publisher as PublisherModel
from tests.transmuters import sqlalchemy_materia

SEED = 42
# Use SQLite in-memory for benchmarks - avoids network I/O for instrumentation mode
DB_URL = "sqlite:///:memory:"


@pytest.fixture(scope="session")
def engine() -> Generator[Engine, None, None]:
    """Create a database engine for benchmark tests."""
    sync_engine = create_engine(
        DB_URL,
        echo=False,
        future=True,
    )

    try:
        yield sync_engine
    finally:
        sync_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def setup_database(engine: Engine) -> None:
    """Set up the database schema."""
    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)


@pytest.fixture(scope="session")
def session_factory(engine: Engine) -> sessionmaker:
    """Create a session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(scope="session")
def arcanum_session_factory(engine: Engine) -> sessionmaker:
    """Create an arcanum session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False, class_=ArcanumSession)


@pytest.fixture
def materia() -> Generator[SqlalchemyMateria, None, None]:
    """Activate sqlalchemy_materia for tests that need it."""
    with sqlalchemy_materia:
        yield sqlalchemy_materia


@pytest.fixture(scope="session")
def seeded_authors(
    session_factory: sessionmaker,
) -> Generator[list[AuthorModel], None, None]:
    """
    Pre-seed the database with authors for read/update/delete benchmarks.

    Creates 100 authors with reproducible data for consistent benchmarks.
    This fixture is session-scoped - data persists across all tests.
    """
    random.seed(SEED)
    session = session_factory()

    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    names = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
    ]

    authors = []
    for _ in range(100):
        author = AuthorModel(
            name=f"{random.choice(names)} {random.randint(100, 999)}",
            field=random.choice(fields),
        )
        session.add(author)
        authors.append(author)

    session.commit()

    for author in authors:
        session.refresh(author)

    yield authors

    session.close()


@pytest.fixture(scope="session")
def seeded_books(
    session_factory: sessionmaker,
    seeded_authors: list[AuthorModel],
) -> Generator[list[BookModel], None, None]:
    """
    Pre-seed the database with books (with relationships) for read benchmarks.

    Creates 100 books linked to authors and publishers.
    """
    random.seed(SEED + 1)
    session = session_factory()

    countries = ["USA", "UK", "Germany", "France", "Japan", "Canada"]
    adjectives = [
        "Galactic",
        "Neon",
        "Stellar",
        "Interstellar",
        "Synthetic",
        "Cybernetic",
    ]
    nouns = ["Odyssey", "Chronicles", "Protocol", "Singularity", "Nexus", "Expedition"]

    publishers = []
    for i in range(10):
        publisher = PublisherModel(
            name=f"Publisher {random.choice(['House', 'Press', 'Books'])} {i}",
            country=random.choice(countries),
        )
        session.add(publisher)
        publishers.append(publisher)

    session.flush()

    books = []
    for i in range(100):
        author = seeded_authors[i % len(seeded_authors)]
        publisher = publishers[i % len(publishers)]

        book = BookModel(
            title=f"The {random.choice(adjectives)} {random.choice(nouns)} {random.randint(1, 99)}",
            year=random.randint(1990, 2024),
            author_id=author.id,
            publisher_id=publisher.id,
        )
        session.add(book)
        books.append(book)

    session.commit()

    for book in books:
        session.refresh(book)

    yield books

    session.close()


@pytest.fixture(scope="session")
def create_author_data() -> list[dict[str, Any]]:
    """Generate author data for create benchmarks."""
    random.seed(SEED + 3)

    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    names = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
    ]

    return [
        {
            "name": f"{random.choice(names)} {random.randint(100, 999)}",
            "write_field": random.choice(fields),
        }
        for _ in range(50)
    ]


@pytest.fixture(scope="session")
def create_nested_book_data() -> list[dict[str, Any]]:
    """Generate book data with nested author and publisher for create benchmarks."""
    random.seed(SEED + 4)

    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    countries = ["USA", "UK", "Germany", "France", "Japan", "Canada"]
    adjectives = [
        "Galactic",
        "Neon",
        "Stellar",
        "Interstellar",
        "Synthetic",
        "Cybernetic",
    ]
    nouns = ["Odyssey", "Chronicles", "Protocol", "Singularity", "Nexus", "Expedition"]
    names = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
    ]

    return [
        {
            "title": f"The {random.choice(adjectives)} {random.choice(nouns)} {random.randint(1, 99)}",
            "year": random.randint(1990, 2024),
            "author": {
                "name": f"{random.choice(names)} {random.randint(100, 999)}",
                "field": random.choice(fields),
            },
            "publisher": {
                "name": f"Publisher {random.choice(['House', 'Press', 'Books'])} {random.randint(1, 50)}",
                "country": random.choice(countries),
            },
        }
        for _ in range(50)
    ]
