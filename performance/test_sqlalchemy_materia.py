"""
Performance Benchmarks: SQLAlchemy Integration CRUD Operations

This module benchmarks arcanum's SQLAlchemy materia against:
1. Pure SQLAlchemy (baseline)
2. Common pattern: Pydantic validate + model_dump to ORM

================================================================================
TEST GROUPS OVERVIEW
================================================================================

CREATE OPERATIONS:
- create-single: Create single objects
- create-bulk: Create 100 objects in batch
- create-nested: Create objects with nested relationships

READ OPERATIONS:
- read-single: Load single object by ID
- read-bulk: Load 50 objects with query
- read-nested: Load with eager-loaded relationships
- read-deep: Load author with all books and book relationships

UPDATE OPERATIONS:
- update-single: Update single object fields
- update-bulk: Update 50 objects
- update-nested: Update with nested relationship changes

DELETE OPERATIONS:
- delete-single: Delete single object
- delete-bulk: Delete 50 objects
- delete-cascade: Delete with cascade to relationships

ROUNDTRIP PATTERNS:
- roundtrip: Load -> Modify -> Save pattern

================================================================================
COMPARISON APPROACHES
================================================================================

1. PURE SQLALCHEMY (Baseline)
   - Direct ORM operations
   - No Pydantic validation layer

2. COMMON PATTERN (Pydantic + model_dump)
   - Validate with Pydantic first
   - model_dump() then create ORM objects
   - This is what most developers do today

3. ARCANUM (with validation)
   - Use transmuter with model_validate
   - Full Pydantic validation

4. ARCANUM (without validation)
   - Use transmuter with model_construct
   - Skip validation, still get ORM integration

Run with: pytest performance/test_sqlalchemy_crud.py -v --benchmark-only
"""

from __future__ import annotations

import random
import uuid
from typing import Literal, Optional

import pytest
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, select
from sqlalchemy.orm import Session as SASession
from sqlalchemy.orm import joinedload, selectinload

from arcanum.association import Relation
from arcanum.materia.sqlalchemy import Session
from tests import models
from tests.transmuters import (
    Author,
    Book,
    BookDetail,
    Publisher,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

SEED = 42
random.seed(SEED)


# ============================================================================
# PURE PYDANTIC MODELS (for common pattern comparison)
# ============================================================================


class PureAuthorSimple(BaseModel):
    """Simple author for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    field: Literal["Physics", "Biology", "Chemistry", "Literature", "History"]


class PurePublisher(BaseModel):
    """Publisher for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    country: str


class PureBookDetail(BaseModel):
    """Book detail for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    isbn: str
    pages: int
    abstract: str


class PureCategory(BaseModel):
    """Category for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    description: Optional[str] = None


class PureReview(BaseModel):
    """Review for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    reviewer_name: str
    rating: int = Field(ge=1, le=5)
    comment: str


class PureBook(BaseModel):
    """Book with nested relationships for common pattern."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    title: str
    year: int
    author: Optional[PureAuthorSimple] = None
    publisher: Optional[PurePublisher] = None
    detail: Optional[PureBookDetail] = None
    categories: list[PureCategory] = Field(default_factory=list)
    reviews: list[PureReview] = Field(default_factory=list)


class PureAuthorWithBooks(BaseModel):
    """Author with books for deep reading."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    field: str
    books: list[PureBook] = Field(default_factory=list)


# ============================================================================
# DATA GENERATORS
# ============================================================================


def generate_author_data(count: int = 100, seed: int = SEED) -> list[dict]:
    """Generate author data for creation tests."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    return [
        {
            "name": f"{random.choice(names)} {random.randint(100, 999)}",
            "field": random.choice(fields),
        }
        for _ in range(count)
    ]


def generate_nested_book_data(count: int = 20, seed: int = SEED) -> list[dict]:
    """Generate book data with nested relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    countries = ["USA", "UK", "Germany", "France", "Japan", "Canada"]
    adjectives = ["Great", "Amazing", "Profound", "Essential", "Complete"]
    nouns = ["Theory", "Guide", "Study", "Analysis", "Introduction"]

    books = []
    for i in range(count):
        num_categories = random.randint(1, 3)
        num_reviews = random.randint(0, 4)
        books.append(
            {
                "title": f"The {random.choice(adjectives)} {random.choice(nouns)} {random.randint(1, 99)}",
                "year": random.randint(1995, 2024),
                "author": {
                    "name": f"Author {random.randint(100, 999)}",
                    "field": random.choice(fields),
                },
                "publisher": {
                    "name": f"Publisher {random.choice(['House', 'Press', 'Books'])} {random.randint(1, 50)}",
                    "country": random.choice(countries),
                },
                "detail": {
                    "isbn": f"978{random.randint(0, 999999999):09d}",
                    "pages": random.randint(100, 600),
                    "abstract": " ".join(
                        [
                            f"Word{random.randint(1, 500)}"
                            for _ in range(random.randint(20, 40))
                        ]
                    ),
                },
                "categories": [
                    {
                        "name": f"TestCat{i}_{j}_{random.randint(1, 9999)}",
                        "description": f"Desc {random.randint(1, 999)}",
                    }
                    for j in range(num_categories)
                ],
                "reviews": [
                    {
                        "reviewer_name": f"Reviewer {random.randint(100, 999)}",
                        "rating": random.randint(1, 5),
                        "comment": f"Comment {random.randint(1, 999)}",
                    }
                    for _ in range(num_reviews)
                ],
            }
        )
    return books


# ============================================================================
# MODULE-LEVEL FIXTURE FOR MATERIA
# ============================================================================


@pytest.fixture(autouse=True)
def activate_materia(materia):
    """Activate SQLAlchemy materia for all tests in this module."""
    yield materia


# ============================================================================
# CREATE OPERATION TESTS
# ============================================================================


class TestCreateSingle:
    """
    Benchmark single object creation.

    All approaches create ONE author object.
    """

    @pytest.fixture
    def author_data(self):
        """Single author data."""
        random.seed(SEED)
        return generate_author_data(1, seed=SEED)[0]

    @pytest.mark.benchmark(group="create-single")
    def test_pure_sqlalchemy_create(
        self, benchmark, db_session: SASession, author_data, test_id
    ):
        """Pure SQLAlchemy: Direct ORM object creation."""
        data = author_data

        def create():
            author = models.Author(
                name=data["name"],
                field=data["field"],
                test_id=test_id,
            )
            db_session.add(author)
            db_session.flush()
            author_id = author.id
            db_session.rollback()
            return author_id

        result = benchmark(create)
        assert result is not None

    @pytest.mark.benchmark(group="create-single")
    def test_common_pattern_create(
        self, benchmark, db_session: SASession, author_data, test_id
    ):
        """Common Pattern: Pydantic validate -> model_dump -> ORM."""
        data = author_data

        def create():
            # Validate with Pydantic
            validated = PureAuthorSimple.model_validate(data)
            # Dump to dict and create ORM
            orm_author = models.Author(
                **validated.model_dump(exclude={"id"}),
                test_id=test_id,
            )
            db_session.add(orm_author)
            db_session.flush()
            author_id = orm_author.id
            db_session.rollback()
            return author_id

        result = benchmark(create)
        assert result is not None

    @pytest.mark.benchmark(group="create-single")
    def test_arcanum_create_with_validation(
        self, benchmark, arcanum_session, author_data, test_id
    ):
        """Arcanum: Create transmuter with validation."""
        data = author_data
        session = arcanum_session

        def create():
            author = Author.model_validate(
                {
                    **data,
                    "test_id": test_id,
                }
            )
            session.add(author)
            session.flush()
            # Revalidate to sync transmuter with ORM (this is the penalty)
            author.revalidate()
            author_id = author.id
            session.rollback()
            return author_id

        result = benchmark(create)
        assert result is not None

    @pytest.mark.benchmark(group="create-single")
    def test_arcanum_create_without_validation(
        self, benchmark, arcanum_session, author_data, test_id
    ):
        """Arcanum: Create transmuter without validation (model_construct)."""
        data = author_data
        session = arcanum_session

        def create():
            author = Author.model_construct(**data, test_id=test_id)
            session.add(author)
            session.flush()
            # Revalidate to sync transmuter with ORM (this is the penalty)
            author.revalidate()
            author_id = author.id
            session.rollback()
            return author_id

        result = benchmark(create)
        assert result is not None


class TestCreateBulk:
    """
    Benchmark bulk object creation (100 objects).

    All approaches create 100 author objects.
    """

    @pytest.fixture
    def bulk_data(self):
        """100 author records."""
        return generate_author_data(100, seed=SEED)

    @pytest.mark.benchmark(group="create-bulk")
    def test_pure_sqlalchemy_bulk_create(self, benchmark, engine, bulk_data, test_id):
        """Pure SQLAlchemy: Bulk ORM object creation."""
        data = bulk_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    authors = [
                        models.Author(name=d["name"], field=d["field"], test_id=test_id)
                        for d in data
                    ]
                    session.add_all(authors)
                    session.rollback()
                    return len(authors)

        result = benchmark(create_all)
        assert result == 100

    @pytest.mark.benchmark(group="create-bulk")
    def test_common_pattern_bulk_create(self, benchmark, engine, bulk_data, test_id):
        """Common Pattern: Validate all -> dump all -> create ORM objects."""
        data = bulk_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    # Validate with Pydantic
                    validated = [PureAuthorSimple.model_validate(d) for d in data]
                    # Create ORM objects
                    authors = [
                        models.Author(**v.model_dump(exclude={"id"}), test_id=test_id)
                        for v in validated
                    ]
                    session.add_all(authors)
                    session.rollback()
                    return len(authors)

        result = benchmark(create_all)
        assert result == 100

    @pytest.mark.benchmark(group="create-bulk")
    def test_arcanum_bulk_create_with_validation(
        self, benchmark, engine, bulk_data, test_id
    ):
        """Arcanum: Bulk transmuter creation with validation."""
        data = bulk_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    transmuters = [
                        Author.model_validate({**d, "test_id": test_id}) for d in data
                    ]
                    for t in transmuters:
                        session.add(t)
                    session.rollback()
                    return len(transmuters)

        result = benchmark(create_all)
        assert result == 100

    @pytest.mark.benchmark(group="create-bulk")
    def test_arcanum_bulk_create_without_validation(
        self, benchmark, engine, bulk_data, test_id
    ):
        """Arcanum: Bulk transmuter creation without validation."""
        data = bulk_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    transmuters = [
                        Author.model_construct(**d, test_id=test_id) for d in data
                    ]
                    for t in transmuters:
                        session.add(t)
                    session.rollback()
                    return len(transmuters)

        result = benchmark(create_all)
        assert result == 100


class TestCreateNested:
    """
    Benchmark nested object creation (Book with Author, Publisher, Detail).

    All approaches create 20 books with nested relationships.
    """

    @pytest.fixture
    def nested_data(self):
        """20 books with nested relationships."""
        return generate_nested_book_data(20, seed=SEED)

    @pytest.mark.benchmark(group="create-nested")
    def test_pure_sqlalchemy_nested_create(
        self, benchmark, engine, nested_data, test_id
    ):
        """Pure SQLAlchemy: Create nested ORM objects."""
        data = nested_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    books = []
                    for d in data:
                        author = models.Author(
                            name=d["author"]["name"],
                            field=d["author"]["field"],
                            test_id=test_id,
                        )
                        publisher = models.Publisher(
                            name=d["publisher"]["name"],
                            country=d["publisher"]["country"],
                            test_id=test_id,
                        )
                        session.add_all([author, publisher])
                        session.flush()

                        book = models.Book(
                            title=d["title"],
                            year=d["year"],
                            author=author,
                            publisher=publisher,
                            test_id=test_id,
                        )
                        session.add(book)
                        session.flush()

                        detail = models.BookDetail(
                            isbn=d["detail"]["isbn"],
                            pages=d["detail"]["pages"],
                            abstract=d["detail"]["abstract"],
                            book=book,
                            test_id=test_id,
                        )
                        session.add(detail)
                        books.append(book)

                    session.rollback()
                    return len(books)

        result = benchmark(create_all)
        assert result == 20

    @pytest.mark.benchmark(group="create-nested")
    def test_common_pattern_nested_create(
        self, benchmark, engine, nested_data, test_id
    ):
        """Common Pattern: Validate nested -> create ORM manually."""
        data = nested_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    books = []
                    for d in data:
                        # Validate with Pydantic
                        validated = PureBook.model_validate(d)

                        # Create ORM objects manually
                        assert validated.author
                        author = models.Author(
                            name=validated.author.name,
                            field=validated.author.field,
                            test_id=test_id,
                        )
                        assert validated.publisher
                        publisher = models.Publisher(
                            name=validated.publisher.name,
                            country=validated.publisher.country,
                            test_id=test_id,
                        )
                        session.add_all([author, publisher])
                        session.flush()

                        book = models.Book(
                            title=validated.title,
                            year=validated.year,
                            author=author,
                            publisher=publisher,
                            test_id=test_id,
                        )
                        session.add(book)
                        session.flush()

                        assert validated.detail
                        detail = models.BookDetail(
                            isbn=validated.detail.isbn,
                            pages=validated.detail.pages,
                            abstract=validated.detail.abstract,
                            book=book,
                            test_id=test_id,
                        )
                        session.add(detail)
                        books.append(book)

                    session.rollback()
                    return len(books)

        result = benchmark(create_all)
        assert result == 20

    @pytest.mark.benchmark(group="create-nested")
    def test_arcanum_nested_create(self, benchmark, engine, nested_data, test_id):
        """Arcanum: Create nested transmuters with associations."""
        data = nested_data

        def create_all():
            with Session(engine) as session:
                with session.begin():
                    books = []
                    for d in data:
                        # Create nested transmuters using model_validate
                        author_t = Author.model_validate(
                            {**d["author"], "test_id": test_id}
                        )
                        publisher_t = Publisher.model_validate(
                            {**d["publisher"], "test_id": test_id}
                        )
                        detail_t = BookDetail.model_validate(
                            {**d["detail"], "test_id": test_id}
                        )

                        session.add(author_t)
                        session.add(publisher_t)
                        session.flush()

                        book_t = Book.model_validate(
                            {
                                "title": d["title"],
                                "year": d["year"],
                                "author": Relation(author_t),
                                "publisher": Relation(publisher_t),
                                "detail": Relation(detail_t),
                                "test_id": test_id,
                            }
                        )
                        session.add(book_t)
                        books.append(book_t)

                    session.rollback()
                    return len(books)

        result = benchmark(create_all)
        assert result == 20


# ============================================================================
# READ OPERATION TESTS
# ============================================================================


class TestReadSingle:
    """
    Benchmark single object read.

    All approaches read ONE author by ID.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create test data."""
        prefix = str(test_id)[:8]
        author = models.Author(
            name=f"Read Author {prefix}",
            field="Physics",
            test_id=test_id,
        )
        db_session.add(author)
        db_session.commit()
        self.author_id = author.id
        self.test_id = test_id

    @pytest.mark.benchmark(group="read-single")
    def test_pure_sqlalchemy_read(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Get by ID."""
        author_id = self.author_id

        def read():
            # Expunge to avoid identity map caching - ensures fair comparison
            db_session.expunge_all()
            return db_session.get(models.Author, author_id)

        result = benchmark(read)
        assert result is not None
        assert "Read Author" in result.name

    @pytest.mark.benchmark(group="read-single")
    def test_common_pattern_read(self, benchmark, db_session: SASession):
        """Common Pattern: Get ORM -> model_validate."""
        author_id = self.author_id

        def read():
            # Expunge to avoid identity map caching - ensures fair comparison
            db_session.expunge_all()
            orm_author = db_session.get(models.Author, author_id)
            return PureAuthorSimple.model_validate(orm_author)

        result = benchmark(read)
        assert result is not None
        assert "Read Author" in result.name

    @pytest.mark.benchmark(group="read-single")
    def test_arcanum_read_with_validation(self, benchmark, arcanum_session):
        """Arcanum: Get ORM -> transmuter with validation."""
        author_id = self.author_id
        session = arcanum_session

        def read():
            # Expunge to avoid identity map caching - ensures fair comparison
            session.expunge_all()
            return session.get(Author, author_id)

        result = benchmark(read)
        assert result is not None
        assert "Read Author" in result.name

    @pytest.mark.benchmark(group="read-single")
    def test_arcanum_read_without_validation(self, benchmark, arcanum_session):
        """Arcanum: Get ORM -> transmuter with model_construct."""
        author_id = self.author_id
        session = arcanum_session

        def read():
            # Expunge to avoid identity map caching - ensures fair comparison
            session.expunge_all()
            orm_author = session.get(models.Author, author_id)
            return Author.model_construct(data=orm_author)

        result = benchmark(read)
        assert result is not None
        assert "Read Author" in result.name


class TestReadBulk:
    """
    Benchmark bulk object read (50 objects).

    All approaches read 50 authors with a query.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create 100 test authors."""
        prefix = str(test_id)[:8]
        fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
        for i in range(100):
            author = models.Author(
                name=f"Bulk Read {prefix}-{i}",
                field=fields[i % len(fields)],
                test_id=test_id,
            )
            db_session.add(author)
        db_session.commit()
        self.test_id = test_id

    @pytest.mark.benchmark(group="read-bulk")
    def test_pure_sqlalchemy_bulk_read(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Query 50 authors."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Author).where(models.Author.test_id == test_id).limit(50)
            )
            return db_session.execute(stmt).scalars().all()

        result = benchmark(read_all)
        assert len(result) == 50

    @pytest.mark.benchmark(group="read-bulk")
    def test_common_pattern_bulk_read(self, benchmark, db_session: SASession):
        """Common Pattern: Query ORM -> model_validate each."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Author).where(models.Author.test_id == test_id).limit(50)
            )
            orm_authors = db_session.execute(stmt).scalars().all()
            return [PureAuthorSimple.model_validate(a) for a in orm_authors]

        result = benchmark(read_all)
        assert len(result) == 50

    @pytest.mark.benchmark(group="read-bulk")
    def test_arcanum_bulk_read_with_validation(self, benchmark, arcanum_session):
        """Arcanum: Query ORM -> transmuter with validation."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = select(Author).where(Author["test_id"] == test_id).limit(50)
            return session.scalars(stmt).all()

        result = benchmark(read_all)
        assert len(result) == 50

    @pytest.mark.benchmark(group="read-bulk")
    def test_arcanum_bulk_read_without_validation(self, benchmark, arcanum_session):
        """Arcanum: Query ORM -> transmuter with model_construct."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = (
                select(models.Author).where(models.Author.test_id == test_id).limit(50)
            )
            orm_authors = session.execute(stmt).scalars().all()
            return [Author.model_construct(data=a) for a in orm_authors]

        result = benchmark(read_all)
        assert len(result) == 50


class TestReadNested:
    """
    Benchmark reading with eager-loaded relationships.

    All approaches read 50 books with author, publisher, detail, categories, reviews.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create books with relationships."""
        prefix = str(test_id)[:8]
        fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]

        publishers = [
            models.Publisher(
                name=f"Pub {prefix}-{i}", country=f"Country {i}", test_id=test_id
            )
            for i in range(5)
        ]
        db_session.add_all(publishers)
        db_session.flush()

        for i in range(10):
            author = models.Author(
                name=f"Nested Author {prefix}-{i}",
                field=fields[i % len(fields)],
                test_id=test_id,
            )
            db_session.add(author)
            db_session.flush()

            for j in range(10):
                book = models.Book(
                    title=f"Nested Book {prefix}-{i}-{j}",
                    year=2000 + j,
                    author=author,
                    publisher=publishers[j % len(publishers)],
                    test_id=test_id,
                )
                db_session.add(book)
                db_session.flush()

                detail = models.BookDetail(
                    isbn=f"{prefix[:6]}{i:02d}{j:02d}",
                    pages=100 + j * 10,
                    abstract=f"Abstract {i}-{j}",
                    book=book,
                    test_id=test_id,
                )
                db_session.add(detail)

                for k in range(2):
                    review = models.Review(
                        reviewer_name=f"Rev {prefix}-{i}-{j}-{k}",
                        rating=1 + (k % 5),
                        comment=f"Comment {k}",
                        book=book,
                        test_id=test_id,
                    )
                    db_session.add(review)

        db_session.commit()
        self.test_id = test_id

    @pytest.mark.benchmark(group="read-nested")
    def test_pure_sqlalchemy_nested_read(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Eager load all relationships."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Book)
                .where(models.Book.test_id == test_id)
                .options(
                    joinedload(models.Book.author),
                    joinedload(models.Book.publisher),
                    joinedload(models.Book.detail),
                    selectinload(models.Book.reviews),
                )
                .limit(50)
            )
            return db_session.execute(stmt).unique().scalars().all()

        result = benchmark(read_all)
        assert len(result) == 50
        assert result[0].author is not None

    @pytest.mark.benchmark(group="read-nested")
    def test_common_pattern_nested_read(self, benchmark, db_session: SASession):
        """Common Pattern: Eager load -> manual dict -> model_validate."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Book)
                .where(models.Book.test_id == test_id)
                .options(
                    joinedload(models.Book.author),
                    joinedload(models.Book.publisher),
                    joinedload(models.Book.detail),
                    selectinload(models.Book.reviews),
                )
                .limit(50)
            )
            orm_books = db_session.execute(stmt).unique().scalars().all()

            results = []
            for b in orm_books:
                book_data = {
                    "id": b.id,
                    "title": b.title,
                    "year": b.year,
                    "author": {
                        "id": b.author.id,
                        "name": b.author.name,
                        "field": b.author.field,
                    }
                    if b.author
                    else None,
                    "publisher": {
                        "id": b.publisher.id,
                        "name": b.publisher.name,
                        "country": b.publisher.country,
                    }
                    if b.publisher
                    else None,
                    "detail": {
                        "id": b.detail.id,
                        "isbn": b.detail.isbn,
                        "pages": b.detail.pages,
                        "abstract": b.detail.abstract,
                    }
                    if b.detail
                    else None,
                    "categories": [],
                    "reviews": [
                        {
                            "id": r.id,
                            "reviewer_name": r.reviewer_name,
                            "rating": r.rating,
                            "comment": r.comment,
                        }
                        for r in b.reviews
                    ],
                }
                results.append(PureBook.model_validate(book_data))
            return results

        result = benchmark(read_all)
        assert len(result) == 50

    @pytest.mark.benchmark(group="read-nested")
    def test_arcanum_nested_read_with_validation(self, benchmark, arcanum_session):
        """Arcanum: Eager load -> transmuter with validation."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = (
                select(Book)
                .where(Book["test_id"] == test_id)
                .options(
                    joinedload(models.Book.author),
                    joinedload(models.Book.publisher),
                    joinedload(models.Book.detail),
                    selectinload(models.Book.reviews),
                )
                .limit(50)
            )
            return session.scalars(stmt).unique().all()

        result = benchmark(read_all)
        assert len(result) == 50

    @pytest.mark.benchmark(group="read-nested")
    def test_arcanum_nested_read_without_validation(self, benchmark, arcanum_session):
        """Arcanum: Eager load -> transmuter with model_construct."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = (
                select(models.Book)
                .where(models.Book.test_id == test_id)
                .options(
                    joinedload(models.Book.author),
                    joinedload(models.Book.publisher),
                    joinedload(models.Book.detail),
                    selectinload(models.Book.reviews),
                )
                .limit(50)
            )
            orm_books = session.execute(stmt).unique().scalars().all()
            return [Book.model_construct(data=b) for b in orm_books]

        result = benchmark(read_all)
        assert len(result) == 50


class TestReadDeep:
    """
    Benchmark reading deeply nested relationships.

    Load 10 authors with all their books and book relationships.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create deep nested data."""
        prefix = str(test_id)[:8]
        fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]

        publishers = [
            models.Publisher(
                name=f"Deep Pub {prefix}-{i}", country=f"Country {i}", test_id=test_id
            )
            for i in range(5)
        ]
        db_session.add_all(publishers)
        db_session.flush()

        for i in range(20):
            author = models.Author(
                name=f"Deep Author {prefix}-{i}",
                field=fields[i % len(fields)],
                test_id=test_id,
            )
            db_session.add(author)
            db_session.flush()

            for j in range(15):
                book = models.Book(
                    title=f"Deep Book {prefix}-{i}-{j}",
                    year=2000 + j,
                    author=author,
                    publisher=publishers[j % len(publishers)],
                    test_id=test_id,
                )
                db_session.add(book)
                db_session.flush()

                detail = models.BookDetail(
                    isbn=f"{prefix[:4]}{i:03d}{j:03d}",
                    pages=100 + j * 10,
                    abstract=f"Deep abstract {i}-{j}",
                    book=book,
                    test_id=test_id,
                )
                db_session.add(detail)

        db_session.commit()
        self.test_id = test_id

    @pytest.mark.benchmark(group="read-deep")
    def test_pure_sqlalchemy_deep_read(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Load author with all nested books."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Author)
                .where(models.Author.test_id == test_id)
                .options(
                    selectinload(models.Author.books).joinedload(models.Book.publisher),
                    selectinload(models.Author.books).joinedload(models.Book.detail),
                )
                .limit(10)
            )
            return db_session.execute(stmt).unique().scalars().all()

        result = benchmark(read_all)
        assert len(result) == 10
        assert len(result[0].books) > 0

    @pytest.mark.benchmark(group="read-deep")
    def test_common_pattern_deep_read(self, benchmark, db_session: SASession):
        """Common Pattern: Load ORM -> manual conversion."""
        test_id = self.test_id

        def read_all():
            stmt = (
                select(models.Author)
                .where(models.Author.test_id == test_id)
                .options(
                    selectinload(models.Author.books).joinedload(models.Book.publisher),
                    selectinload(models.Author.books).joinedload(models.Book.detail),
                )
                .limit(10)
            )
            orm_authors = db_session.execute(stmt).unique().scalars().all()

            results = []
            for a in orm_authors:
                author_data = {
                    "id": a.id,
                    "name": a.name,
                    "field": a.field,
                    "books": [
                        {
                            "id": b.id,
                            "title": b.title,
                            "year": b.year,
                            "publisher": {
                                "id": b.publisher.id,
                                "name": b.publisher.name,
                                "country": b.publisher.country,
                            }
                            if b.publisher
                            else None,
                            "detail": {
                                "id": b.detail.id,
                                "isbn": b.detail.isbn,
                                "pages": b.detail.pages,
                                "abstract": b.detail.abstract,
                            }
                            if b.detail
                            else None,
                            "categories": [],
                            "reviews": [],
                        }
                        for b in a.books
                    ],
                }
                results.append(PureAuthorWithBooks.model_validate(author_data))
            return results

        result = benchmark(read_all)
        assert len(result) == 10

    @pytest.mark.benchmark(group="read-deep")
    def test_arcanum_deep_read_with_validation(self, benchmark, arcanum_session):
        """Arcanum: Deep load with validation."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = (
                select(Author)
                .where(Author["test_id"] == test_id)
                .options(
                    selectinload(models.Author.books).joinedload(models.Book.publisher),
                    selectinload(models.Author.books).joinedload(models.Book.detail),
                )
                .limit(10)
            )
            return session.scalars(stmt).unique().all()

        result = benchmark(read_all)
        assert len(result) == 10

    @pytest.mark.benchmark(group="read-deep")
    def test_arcanum_deep_read_without_validation(self, benchmark, arcanum_session):
        """Arcanum: Deep load without validation."""
        test_id = self.test_id
        session = arcanum_session

        def read_all():
            stmt = (
                select(models.Author)
                .where(models.Author.test_id == test_id)
                .options(
                    selectinload(models.Author.books).joinedload(models.Book.publisher),
                    selectinload(models.Author.books).joinedload(models.Book.detail),
                )
                .limit(10)
            )
            orm_authors = session.execute(stmt).unique().scalars().all()
            return [Author.model_construct(data=a) for a in orm_authors]

        result = benchmark(read_all)
        assert len(result) == 10


# ============================================================================
# UPDATE OPERATION TESTS
# ============================================================================


class TestUpdateSingle:
    """
    Benchmark single object update.

    All approaches update ONE author's name.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create test author."""
        prefix = str(test_id)[:8]
        author = models.Author(
            name=f"Update Author {prefix}", field="Physics", test_id=test_id
        )
        db_session.add(author)
        db_session.commit()
        self.author_id = author.id
        self.test_id = test_id

    @pytest.mark.benchmark(group="update-single")
    def test_pure_sqlalchemy_update(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Direct ORM update."""
        author_id = self.author_id

        def do_update():
            author = db_session.get(models.Author, author_id)
            assert author
            author.name = f"Updated {random.randint(1, 9999)}"
            db_session.flush()
            db_session.rollback()
            return author.id

        result = benchmark(do_update)
        assert result is not None

    @pytest.mark.benchmark(group="update-single")
    def test_common_pattern_update(self, benchmark, db_session: SASession):
        """Common Pattern: Load ORM -> validate -> update ORM."""
        author_id = self.author_id

        def do_update():
            orm_author = db_session.get(models.Author, author_id)
            validated = PureAuthorSimple.model_validate(orm_author)
            new_data = validated.model_dump()
            new_data["name"] = f"Updated {random.randint(1, 9999)}"
            validated_new = PureAuthorSimple.model_validate(new_data)
            assert orm_author
            orm_author.name = validated_new.name
            db_session.flush()
            db_session.rollback()
            return orm_author.id

        result = benchmark(do_update)
        assert result is not None

    @pytest.mark.benchmark(group="update-single")
    def test_arcanum_update_with_validation(self, benchmark, arcanum_session):
        """Arcanum: Load as transmuter -> update -> sync."""
        author_id = self.author_id
        session = arcanum_session

        def do_update():
            transmuter = session.get(Author, author_id)
            # In real usage, updates on transmuter sync with ORM
            # For benchmark, we simulate the pattern
            transmuter.name = f"Updated {random.randint(1, 9999)}"
            session.flush()
            session.rollback()
            return transmuter.id

        result = benchmark(do_update)
        assert result is not None


class TestUpdateBulk:
    """
    Benchmark bulk object updates (50 objects).
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, test_id: uuid.UUID):
        """Create 100 authors."""
        prefix = str(test_id)[:8]
        fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
        for i in range(100):
            author = models.Author(
                name=f"Bulk Update {prefix}-{i}",
                field=fields[i % len(fields)],
                test_id=test_id,
            )
            db_session.add(author)
        db_session.commit()
        self.test_id = test_id

    @pytest.mark.benchmark(group="update-bulk")
    def test_pure_sqlalchemy_bulk_update(self, benchmark, db_session: SASession):
        """Pure SQLAlchemy: Bulk ORM update."""
        test_id = self.test_id

        def do_update():
            stmt = (
                select(models.Author).where(models.Author.test_id == test_id).limit(50)
            )
            authors = db_session.execute(stmt).scalars().all()
            for author in authors:
                author.name = f"Bulk Updated {random.randint(1, 9999)}"
            db_session.flush()
            db_session.rollback()
            return len(authors)

        result = benchmark(do_update)
        assert result == 50

    @pytest.mark.benchmark(group="update-bulk")
    def test_common_pattern_bulk_update(self, benchmark, db_session: SASession):
        """Common Pattern: Load -> validate -> update each."""
        test_id = self.test_id

        def do_update():
            stmt = (
                select(models.Author).where(models.Author.test_id == test_id).limit(50)
            )
            authors = db_session.execute(stmt).scalars().all()
            for orm_author in authors:
                validated = PureAuthorSimple.model_validate(orm_author)
                new_data = validated.model_dump()
                new_data["name"] = f"Bulk Updated {random.randint(1, 9999)}"
                validated_new = PureAuthorSimple.model_validate(new_data)
                orm_author.name = validated_new.name
            db_session.flush()
            db_session.rollback()
            return len(authors)

        result = benchmark(do_update)
        assert result == 50

    @pytest.mark.benchmark(group="update-bulk")
    def test_arcanum_bulk_update(self, benchmark, arcanum_session):
        """Arcanum: Load as transmuters -> update."""
        test_id = self.test_id
        session = arcanum_session

        def do_update():
            stmt = select(Author).where(Author["test_id"] == test_id).limit(50)
            transmuters = session.scalars(stmt).all()
            for t in transmuters:
                t.name = f"Bulk Updated {random.randint(1, 9999)}"
            session.flush()
            session.rollback()
            return len(transmuters)

        result = benchmark(do_update)
        assert result == 50


# ============================================================================
# DELETE OPERATION TESTS
# ============================================================================


class TestDeleteSingle:
    """
    Benchmark single object deletion.
    """

    @pytest.fixture
    def create_author(self, db_session: SASession, test_id: uuid.UUID):
        """Create a fresh author for each benchmark iteration."""

        def _create():
            author = models.Author(
                name=f"Delete Me {random.randint(1, 9999)}",
                field="Physics",
                test_id=test_id,
            )
            db_session.add(author)
            db_session.flush()
            return author.id

        return _create

    @pytest.mark.benchmark(group="delete-single")
    def test_pure_sqlalchemy_delete(
        self, benchmark, db_session: SASession, create_author
    ):
        """Pure SQLAlchemy: Delete by ID."""

        def do_delete():
            author_id = create_author()
            author = db_session.get(models.Author, author_id)
            db_session.delete(author)
            db_session.flush()
            db_session.rollback()
            return author_id

        result = benchmark(do_delete)
        assert result is not None

    @pytest.mark.benchmark(group="delete-single")
    def test_common_pattern_delete(
        self, benchmark, db_session: SASession, create_author
    ):
        """Common Pattern: Load -> validate existence -> delete."""

        def do_delete():
            author_id = create_author()
            author = db_session.get(models.Author, author_id)
            # Validate before delete (common pattern to ensure data integrity)
            _ = PureAuthorSimple.model_validate(author)
            db_session.delete(author)
            db_session.flush()
            db_session.rollback()
            return author_id

        result = benchmark(do_delete)
        assert result is not None

    @pytest.mark.benchmark(group="delete-single")
    def test_arcanum_delete(self, benchmark, arcanum_session, db_session, test_id):
        """Arcanum: Load as transmuter -> delete underlying ORM."""
        session = arcanum_session

        def create_author():
            author = models.Author(
                name=f"Delete Me {random.randint(1, 9999)}",
                field="Physics",
                test_id=test_id,
            )
            db_session.add(author)
            db_session.flush()
            db_session.commit()
            return author.id

        def do_delete():
            author_id = create_author()
            transmuter = session.get(Author, author_id)
            # Delete the transmuter (session.delete handles it)
            session.delete(transmuter)
            session.flush()
            session.rollback()
            return author_id

        result = benchmark(do_delete)
        assert result is not None


class TestDeleteBulk:
    """
    Benchmark bulk deletion (50 objects).
    """

    @pytest.fixture
    def create_authors(self, db_session: SASession, test_id: uuid.UUID):
        """Create 50 authors for each benchmark iteration."""

        def _create():
            authors = []
            for _ in range(50):
                author = models.Author(
                    name=f"Bulk Delete {random.randint(1, 9999)}",
                    field="Physics",
                    test_id=test_id,
                )
                db_session.add(author)
                authors.append(author)
            db_session.flush()
            return [a.id for a in authors]

        return _create

    @pytest.mark.benchmark(group="delete-bulk")
    def test_pure_sqlalchemy_bulk_delete(
        self, benchmark, db_session: SASession, create_authors, test_id
    ):
        """Pure SQLAlchemy: Bulk delete."""

        def do_delete():
            author_ids = create_authors()
            stmt = delete(models.Author).where(models.Author.id.in_(author_ids))
            db_session.execute(stmt)
            db_session.flush()
            db_session.rollback()
            return len(author_ids)

        result = benchmark(do_delete)
        assert result == 50

    @pytest.mark.benchmark(group="delete-bulk")
    def test_common_pattern_bulk_delete(
        self, benchmark, db_session: SASession, create_authors, test_id
    ):
        """Common Pattern: Load all -> validate -> delete."""

        def do_delete():
            author_ids = create_authors()
            stmt = select(models.Author).where(models.Author.id.in_(author_ids))
            authors = db_session.execute(stmt).scalars().all()
            # Validate all before delete
            _ = [PureAuthorSimple.model_validate(a) for a in authors]
            for a in authors:
                db_session.delete(a)
            db_session.flush()
            db_session.rollback()
            return len(authors)

        result = benchmark(do_delete)
        assert result == 50

    @pytest.mark.benchmark(group="delete-bulk")
    def test_arcanum_bulk_delete(self, benchmark, arcanum_session, db_session, test_id):
        """Arcanum: Load as transmuters -> delete underlying ORM objects."""
        session = arcanum_session

        def create_authors():
            authors = []
            for _ in range(50):
                author = models.Author(
                    name=f"Bulk Delete {random.randint(1, 9999)}",
                    field="Physics",
                    test_id=test_id,
                )
                db_session.add(author)
                authors.append(author)
            db_session.flush()
            db_session.commit()
            return [a.id for a in authors]

        def do_delete():
            author_ids = create_authors()
            stmt = select(Author).where(Author["id"].in_(author_ids))
            transmuters = session.scalars(stmt).all()
            for t in transmuters:
                session.delete(t)
            session.flush()
            session.rollback()
            return len(transmuters)

        result = benchmark(do_delete)
        assert result == 50


# ============================================================================
# ROUNDTRIP TESTS
# ============================================================================


class TestRoundtrip:
    """
    Benchmark complete roundtrip: Load -> Modify -> Save.
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, db_session: SASession, seeded_database):
        """Use seeded database."""
        pass

    @pytest.mark.benchmark(group="roundtrip")
    def test_common_pattern_roundtrip(self, benchmark, db_session: SASession):
        """Common Pattern: ORM -> Pydantic -> Modify -> Pydantic -> ORM."""

        def roundtrip():
            # Load
            stmt = select(models.Author).limit(10)
            orm_authors = db_session.execute(stmt).scalars().all()

            # Convert to Pydantic
            pydantic_authors = [PureAuthorSimple.model_validate(a) for a in orm_authors]

            # Modify
            modified = []
            for p in pydantic_authors:
                data = p.model_dump()
                data["name"] = f"Modified {data['name'][:20]}"
                modified.append(PureAuthorSimple.model_validate(data))

            # Create new ORM objects (simulating update pattern)
            new_orm = [models.Author(name=m.name, field=m.field) for m in modified]
            return new_orm

        result = benchmark(roundtrip)
        assert len(result) == 10
        assert result[0].name.startswith("Modified")

    @pytest.mark.benchmark(group="roundtrip")
    def test_arcanum_roundtrip(self, benchmark, arcanum_session):
        """Arcanum: Load as transmuter -> Modify directly."""
        session = arcanum_session

        def roundtrip():
            # Load as transmuters
            stmt = select(Author).limit(10)
            transmuters = session.scalars(stmt).all()

            # Modify (transmuters sync with underlying ORM)
            for t in transmuters:
                t.name = f"Modified {t.name[:20]}"

            # Access ORM objects
            return [t.__transmuter_provided__ for t in transmuters]

        result = benchmark(roundtrip)
        assert len(result) == 10
