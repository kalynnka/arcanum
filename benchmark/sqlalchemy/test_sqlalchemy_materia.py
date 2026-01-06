"""
SQLAlchemy Materia Benchmark Tests - ORM CRUD Operations

This module benchmarks arcanum's SQLAlchemy materia against common patterns
for CRUD API endpoints. Comparing three approaches:

1. PURE SQLALCHEMY (Baseline)
   - Direct ORM operations without Pydantic validation

2. PYDANTIC + SQLALCHEMY (Common Pattern)
   - Validate with Pydantic first, then model_dump to create ORM objects
   - This is what most developers do today

3. ARCANUM (Combined)
   - Use transmuter with full Pydantic validation and SQLAlchemy integration
   - Validates and produces ORM-integrated objects in one step

Each test uses transactions that rollback to avoid polluting test data.

Run locally:  pytest benchmark/ -v --benchmark-enable
With CodSpeed: pytest benchmark/ --codspeed
"""

from __future__ import annotations

import random

import pytest
from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload

from tests import models, schemas
from tests.transmuters import Author, Book

# Batch size for benchmark tests
BATCH_SIZE = 50


class TestCreateSingleAuthor:
    """
    Benchmark single author creation (flat object, no relationships).

    Simulates: POST /api/authors with simple payload
    Each iteration creates one author from random selection.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="create-single-author")
    def test_sqlalchemy_create(self, benchmark, session_factory, create_author_data):
        """Pure SQLAlchemy: Direct ORM creation."""
        data = random.choice(create_author_data)
        author_data = {"name": data["name"], "field": data["write_field"]}

        def create():
            with session_factory() as session:
                author = models.Author(**author_data)
                session.add(author)
                session.flush()
                session.rollback()

        benchmark(create)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="create-single-author")
    def test_pydantic_sqlalchemy_create(
        self, benchmark, session_factory, create_author_data
    ):
        """Pydantic + SQLAlchemy: Validate then create."""
        author_data = random.choice(create_author_data)

        def create():
            with session_factory() as session:
                # Validate with Pydantic
                validated = schemas.AuthorCreate.model_validate(author_data)
                # Dump to dict and create ORM
                orm_author = models.Author(**validated.model_dump())
                session.add(orm_author)
                session.flush()
                session.rollback()

        benchmark(create)

    @pytest.mark.benchmark(group="create-single-author")
    def test_arcanum_create(
        self, benchmark, arcanum_session_factory, materia, create_author_data
    ):
        """Arcanum: Transmuter creation."""
        data = random.choice(create_author_data)
        author_data = {"name": data["name"], "field": data["write_field"]}

        def create():
            with arcanum_session_factory() as session:
                author = Author(**author_data)
                session.add(author)
                session.flush()
                session.rollback()

        benchmark(create)


class TestCreateNestedBook:
    """
    Benchmark book creation with nested author and publisher.

    Simulates: POST /api/books with nested author/publisher data
    Each iteration creates one book with nested objects from random selection.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="create-nested-book")
    def test_sqlalchemy_create_nested(
        self, benchmark, session_factory, create_nested_book_data
    ):
        """Pure SQLAlchemy: Create with nested objects."""
        book_data = random.choice(create_nested_book_data)

        def create():
            with session_factory() as session:
                # Create nested objects manually
                author = models.Author(**book_data["author"])
                publisher = models.Publisher(**book_data["publisher"])
                book = models.Book(
                    title=book_data["title"],
                    year=book_data["year"],
                    author=author,
                    publisher=publisher,
                )
                session.add(book)
                session.flush()
                session.rollback()

        benchmark(create)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="create-nested-book")
    def test_pydantic_sqlalchemy_create_nested(
        self, benchmark, session_factory, create_nested_book_data
    ):
        """Pydantic + SQLAlchemy: Validate then create."""
        book_data = random.choice(create_nested_book_data)

        def create():
            with session_factory() as session:
                # Validate with Pydantic (validates nested structures too)
                validated = schemas.BookCreate.model_validate(book_data)
                # Create ORM objects from validated data
                author = models.Author(**validated.author.model_dump())
                publisher = models.Publisher(**validated.publisher.model_dump())
                book = models.Book(
                    title=validated.title,
                    year=validated.year,
                    author=author,
                    publisher=publisher,
                )
                session.add(book)
                session.flush()
                session.rollback()

        benchmark(create)

    @pytest.mark.benchmark(group="create-nested-book")
    def test_arcanum_create_nested(
        self, benchmark, arcanum_session_factory, materia, create_nested_book_data
    ):
        """Arcanum: Transmuter with nested objects."""
        book_data = random.choice(create_nested_book_data)

        def create():
            with arcanum_session_factory() as session:
                book = Book(**book_data)
                session.add(book)
                session.flush()
                session.rollback()

        benchmark(create)


class TestReadSingleAuthor:
    """
    Benchmark reading a single author by ID.

    Simulates: GET /api/authors/{id}
    Each iteration reads one randomly selected author.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-single-author")
    def test_sqlalchemy_read(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pure SQLAlchemy: Query by ID."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def read():
            with session_factory() as session:
                result = session.get(models.Author, author_id)
                assert result is not None

        benchmark(read)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-single-author")
    def test_pydantic_sqlalchemy_read(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pydantic + SQLAlchemy: Query then validate."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def read():
            with session_factory() as session:
                orm_author = session.get(models.Author, author_id)
                # Convert to Pydantic for API response
                validated = schemas.AuthorFlat.model_validate(orm_author)
                assert validated is not None

        benchmark(read)

    @pytest.mark.benchmark(group="read-single-author")
    def test_arcanum_read(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Query returns transmuter."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def read():
            with arcanum_session_factory() as session:
                result = session.get(Author, author_id)
                assert result is not None

        benchmark(read)


class TestReadManyAuthors:
    """
    Benchmark reading multiple authors with list query.

    Simulates: GET /api/authors?limit=50
    Each iteration fetches 50 authors.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-many-authors")
    def test_sqlalchemy_read_many(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pure SQLAlchemy: Query list."""

        def read():
            with session_factory() as session:
                stmt = (
                    select(models.Author)
                    .options(selectinload(models.Author.books))
                    .limit(BATCH_SIZE)
                )
                result = session.scalars(stmt).all()
                assert len(result) == BATCH_SIZE

        benchmark(read)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-many-authors")
    def test_pydantic_sqlalchemy_read_many(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pydantic + SQLAlchemy: Query then validate list."""

        def read():
            with session_factory() as session:
                stmt = (
                    select(models.Author)
                    .options(selectinload(models.Author.books))
                    .limit(BATCH_SIZE)
                )
                orm_authors = session.scalars(stmt).all()
                # Convert to Pydantic for API response
                validated = [schemas.AuthorFlat.model_validate(a) for a in orm_authors]
                assert len(validated) == BATCH_SIZE

        benchmark(read)

    @pytest.mark.benchmark(group="read-many-authors")
    def test_arcanum_read_many(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Query returns transmuters."""

        def read():
            with arcanum_session_factory() as session:
                stmt = (
                    select(Author)
                    .options(selectinload(Author["books"]))
                    .limit(BATCH_SIZE)
                )
                result = session.scalars(stmt).all()
                assert len(result) == BATCH_SIZE

        benchmark(read)


class TestReadNestedBook:
    """
    Benchmark reading books with eager-loaded relationships.

    Simulates: GET /api/books/{id} with author and publisher included
    Each iteration reads one randomly selected book with relationships.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-nested-book")
    def test_sqlalchemy_read_nested(
        self, benchmark, session_factory, seeded_books: list[models.Book]
    ):
        """Pure SQLAlchemy: Query with joinedload."""
        book = random.choice(seeded_books)
        book_id = book.id

        def read():
            with session_factory() as session:
                stmt = (
                    select(models.Book)
                    .where(models.Book.id == book_id)
                    .options(
                        joinedload(models.Book.author),
                        joinedload(models.Book.publisher),
                    )
                )
                result = session.scalars(stmt).first()
                assert result is not None
                assert result.author is not None
                assert result.publisher is not None

        benchmark(read)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="read-nested-book")
    def test_pydantic_sqlalchemy_read_nested(
        self, benchmark, session_factory, seeded_books: list[models.Book]
    ):
        """Pydantic + SQLAlchemy: Query then validate."""
        book = random.choice(seeded_books)
        book_id = book.id

        def read():
            with session_factory() as session:
                stmt = (
                    select(models.Book)
                    .where(models.Book.id == book_id)
                    .options(
                        joinedload(models.Book.author),
                        joinedload(models.Book.publisher),
                    )
                )
                orm_book = session.scalars(stmt).first()
                # Convert to Pydantic for API response
                validated = schemas.BookFlat.model_validate(orm_book)
                assert validated is not None
                assert validated.author is not None

        benchmark(read)

    @pytest.mark.benchmark(group="read-nested-book")
    def test_arcanum_read_nested(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_books: list[models.Book],
    ):
        """Arcanum: Query with eager load."""
        book = random.choice(seeded_books)
        book_id = book.id

        def read():
            with arcanum_session_factory() as session:
                stmt = (
                    select(Book)
                    .where(Book["id"] == book_id)
                    .options(
                        joinedload(Book["author"]),
                        joinedload(Book["publisher"]),
                    )
                )
                result = session.scalars(stmt).first()
                assert result is not None
                assert result.author.value is not None

        benchmark(read)


class TestUpdateSingleAuthor:
    """
    Benchmark updating a single author.

    Simulates: PUT/PATCH /api/authors/{id}
    Each iteration updates one randomly selected author.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="update-single-author")
    def test_sqlalchemy_update(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pure SQLAlchemy: Direct ORM update."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def update():
            with session_factory() as session:
                orm_author = session.get(models.Author, author_id)
                orm_author.name = "Updated Name"
                orm_author.field = "Physics"
                session.flush()
                session.rollback()

        benchmark(update)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="update-single-author")
    def test_pydantic_sqlalchemy_update(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pydantic + SQLAlchemy: Validate then update."""
        author = random.choice(seeded_authors)
        author_id = author.id
        update_data = {"name": "Updated Name", "write_field": "Physics"}

        def update():
            with session_factory() as session:
                # Validate update data
                validated = schemas.AuthorCreate.model_validate(update_data)
                # Fetch and update ORM object
                orm_author = session.get(models.Author, author_id)
                for k, v in validated.model_dump(
                    by_alias=True, exclude_unset=True
                ).items():
                    setattr(orm_author, k, v)
                session.flush()
                session.rollback()

        benchmark(update)

    @pytest.mark.benchmark(group="update-single-author")
    def test_arcanum_update(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Transmuter update."""
        author = random.choice(seeded_authors)
        author_id = author.id
        update_data = {"name": "Updated Name", "write_field": "Physics"}
        AuthorUpdate = Author.Update

        def update():
            with arcanum_session_factory() as session:
                updated = AuthorUpdate(**update_data)
                transmuter = session.get(Author, author_id)
                transmuter.absorb(updated)
                session.flush()
                session.rollback()

        benchmark(update)


class TestUpdateManyAuthors:
    """
    Benchmark bulk update of multiple authors.

    Simulates: Batch update endpoint
    Each iteration updates 50 authors.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="update-many-authors")
    def test_sqlalchemy_update_many(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pure SQLAlchemy: Bulk update."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def update():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                for author in authors:
                    author.name = f"Bulk Updated {author.id}"
                    author.field = "Physics"
                session.flush()
                session.rollback()

        benchmark(update)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="update-many-authors")
    def test_pydantic_sqlalchemy_update_many(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pydantic + SQLAlchemy: Validate then update."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def update():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                for author in authors:
                    update_data = {
                        "name": f"Bulk Updated {author.id}",
                        "write_field": "Physics",
                    }
                    validated = schemas.AuthorCreate.model_validate(update_data)
                    for k, v in validated.model_dump(
                        by_alias=True, exclude_unset=True
                    ).items():
                        setattr(author, k, v)
                session.flush()
                session.rollback()

        benchmark(update)

    @pytest.mark.benchmark(group="update-many-authors")
    def test_arcanum_update_many(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Bulk update."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]
        AuthorUpdate = Author.Update

        def update():
            with arcanum_session_factory() as session:
                stmt = select(Author).where(Author["id"].in_(author_ids))
                authors = session.scalars(stmt).all()
                for author in authors:
                    updated = AuthorUpdate(
                        name=f"Bulk Updated {author.id}",
                        write_field="Physics",
                    )
                    author.absorb(updated)
                session.flush()
                session.rollback()

        benchmark(update)


class TestSerializeToDict:
    """
    Benchmark serializing ORM/transmuter to dict for API response.

    Simulates: Converting query result to JSON response
    Each iteration serializes 50 authors.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="serialize-dict")
    def test_sqlalchemy_serialize_dict(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pure SQLAlchemy: Manual dict."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def serialize():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                return [{"id": a.id, "name": a.name, "field": a.field} for a in authors]

        result = benchmark(serialize)
        assert len(result) == BATCH_SIZE

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="serialize-dict")
    def test_pydantic_sqlalchemy_serialize_dict(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pydantic + SQLAlchemy: Validate then dump."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def serialize():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                validated = [schemas.AuthorFlat.model_validate(a) for a in authors]
                return [v.model_dump() for v in validated]

        result = benchmark(serialize)
        assert len(result) == BATCH_SIZE

    @pytest.mark.benchmark(group="serialize-dict")
    def test_arcanum_serialize_dict(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: model_dump."""
        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def serialize():
            with arcanum_session_factory() as session:
                stmt = select(Author).where(Author["id"].in_(author_ids))
                authors = session.scalars(stmt).all()
                return [a.model_dump(exclude={"books", "test_id"}) for a in authors]

        result = benchmark(serialize)
        assert len(result) == BATCH_SIZE


class TestSerializeToJson:
    """
    Benchmark serializing ORM/transmuter to JSON string for API response.

    Simulates: Direct JSON response generation
    Each iteration serializes 50 authors to JSON.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="serialize-json")
    def test_sqlalchemy_serialize_json(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pure SQLAlchemy: Manual JSON conversion via json module."""
        import json

        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]

        def serialize():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                data = [{"id": a.id, "name": a.name, "field": a.field} for a in authors]
                return json.dumps(data)

        result = benchmark(serialize)
        assert len(result) > 0

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="serialize-json")
    def test_pydantic_sqlalchemy_serialize_json(
        self,
        benchmark,
        session_factory,
        seeded_authors: list[models.Author],
    ):
        """Pydantic + SQLAlchemy: Validate then model_dump_json."""
        from pydantic import TypeAdapter

        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]
        adapter = TypeAdapter(list[schemas.AuthorFlat])

        def serialize():
            with session_factory() as session:
                stmt = select(models.Author).where(models.Author.id.in_(author_ids))
                authors = session.scalars(stmt).all()
                validated = [schemas.AuthorFlat.model_validate(a) for a in authors]
                return adapter.dump_json(validated)

        result = benchmark(serialize)
        assert len(result) > 0

    @pytest.mark.benchmark(group="serialize-json")
    def test_arcanum_serialize_json(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Transmuter model_dump_json."""
        from pydantic import TypeAdapter

        author_ids = [a.id for a in seeded_authors[:BATCH_SIZE]]
        adapter = TypeAdapter(list[Author])

        def serialize():
            with arcanum_session_factory() as session:
                stmt = select(Author).where(Author["id"].in_(author_ids))
                authors = session.scalars(stmt).all()
                return adapter.dump_json(
                    authors, exclude={"__all__": {"books", "test_id"}}
                )

        result = benchmark(serialize)
        assert len(result) > 0


class TestRoundtripAuthor:
    """
    Benchmark full roundtrip: Load -> Modify -> Save.

    Simulates: GET /api/authors/{id}, modify, PUT /api/authors/{id}
    Each iteration performs one roundtrip on a randomly selected author.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="roundtrip-author")
    def test_sqlalchemy_roundtrip(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pure SQLAlchemy: Load, modify, flush, rollback."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def roundtrip():
            with session_factory() as session:
                orm_author = session.get(models.Author, author_id)
                orm_author.name = f"Roundtrip Updated {author_id}"
                orm_author.field = "Physics"
                session.flush()
                session.rollback()

        benchmark(roundtrip)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="roundtrip-author")
    def test_pydantic_sqlalchemy_roundtrip(
        self, benchmark, session_factory, seeded_authors: list[models.Author]
    ):
        """Pydantic + SQLAlchemy: Load, validate input, apply, flush."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def roundtrip():
            with session_factory() as session:
                orm_author = session.get(models.Author, author_id)
                update_data = {
                    "name": f"Roundtrip Updated {author_id}",
                    "write_field": "Physics",
                }
                validated = schemas.AuthorCreate.model_validate(update_data)
                orm_author.name = validated.name
                orm_author.field = validated.field
                session.flush()
                session.rollback()

        benchmark(roundtrip)

    @pytest.mark.benchmark(group="roundtrip-author")
    def test_arcanum_roundtrip(
        self,
        benchmark,
        arcanum_session_factory,
        materia,
        seeded_authors: list[models.Author],
    ):
        """Arcanum: Load transmuter, modify, flush."""
        author = random.choice(seeded_authors)
        author_id = author.id

        def roundtrip():
            with arcanum_session_factory() as session:
                transmuter = session.get(Author, author_id)
                transmuter.name = f"Roundtrip Updated {author_id}"
                transmuter.field = "Physics"
                session.flush()
                session.rollback()

        benchmark(roundtrip)
