"""Test Transmuter-ORM integration.

Tests:
- ORM object creation during transmuter initialization
- ORM object creation during validation
- Blessing rules and constraints
- from_attributes and model_validate
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import Engine

from arcanum.materia.sqlalchemy import Session
from arcanum.materia.sqlalchemy.base import SqlalchemyMateria
from tests import models
from tests.transmuters import Author, Book, Publisher


class TestTransmuterORMCreation:
    """Test that transmuters create and manage ORM objects correctly."""

    def test_transmuter_init_creates_orm(self):
        """Test that initializing a transmuter creates an underlying ORM object."""
        author = Author(name="Isaac Asimov", field="Literature")

        assert author.__transmuter_provided__ is not None
        assert isinstance(author.__transmuter_provided__, models.Author)
        assert author.__transmuter_provided__.name == "Isaac Asimov"
        assert author.__transmuter_provided__.field == "Literature"

    def test_transmuter_validation_creates_orm(self):
        """Test that model_validate creates ORM objects."""
        data = {"name": "Arthur C. Clarke", "field": "Literature"}
        author = Author.model_validate(data)

        assert author.__transmuter_provided__ is not None
        assert isinstance(author.__transmuter_provided__, models.Author)
        assert author.name == "Arthur C. Clarke"

    def test_from_attributes_converts_orm_to_transmuter(self, engine: Engine):
        """Test that from_attributes=True allows ORM -> Transmuter conversion."""
        # Create ORM object directly using native SQLAlchemy session
        from sqlalchemy.orm import Session as SASession

        with SASession(engine) as sa_session:
            orm_author = models.Author(name="Philip K. Dick", field="Literature")
            sa_session.add(orm_author)
            sa_session.flush()
            sa_session.commit()
            sa_session.refresh(orm_author)

            # Convert ORM to Transmuter
            author = Author.model_validate(orm_author)

            assert isinstance(author, Author)
            assert author.name == "Philip K. Dick"
            assert author.__transmuter_provided__ is orm_author

    def test_identity_field_readonly_after_set(self, engine: Engine):
        """Test that identity fields are frozen after being set."""
        with Session(engine) as session:
            author = Author(name="Test Author", field="Physics")
            session.add(author)
            session.flush()
            author.revalidate()

            # ID is now set
            assert author.id is not None

            # Try to modify ID - should raise ValidationError
            with pytest.raises(ValidationError, match="frozen"):
                author.id = 999

    def test_transmuter_without_id_can_be_persisted(self, engine: Engine):
        """Test creating a transmuter without ID and persisting it."""
        author = Author(name="New Author", field="Biology")

        assert author.id is None
        assert author.__transmuter_provided__ is not None

        with Session(engine) as session:
            session.add(author)
            session.flush()
            author.revalidate()

            # ID should now be set
            assert author.id is not None
            assert author.__transmuter_provided__.id is not None  # type: ignore

    def test_nested_transmuter_creation(self, engine: Engine):
        """Test creating nested transmuters with relationships."""
        publisher = Publisher(name="Test Publisher", country="USA")
        author = Author(name="Test Author", field="Chemistry")

        # Create book with relationships
        book = Book(
            title="Test Book",
            year=2024,
        )
        book.author.value = author
        book.publisher.value = publisher

        # All should have ORM objects
        assert book.__transmuter_provided__ is not None
        assert author.__transmuter_provided__ is not None
        assert publisher.__transmuter_provided__ is not None

        # Persist
        with Session(engine) as session:
            session.add(book)
            session.flush()
            book.revalidate()
            author.revalidate()
            publisher.revalidate()

            assert book.id is not None
            assert author.id is not None
            assert publisher.id is not None


class TestBlessingRules:
    """Test transmuter blessing constraints and rules."""

    def test_transmuter_blessed_once(self):
        """Test that a transmuter class can only be blessed with one ORM type."""
        from arcanum.base import BaseTransmuter

        # This should work - fresh materia
        test_materia = SqlalchemyMateria()

        @test_materia.bless(models.Author)
        class TestAuthor(BaseTransmuter):
            id: int | None = None
            name: str
            field: str

        # Attempting to bless same class again should fail
        with pytest.raises(RuntimeError, match="already blessed"):
            test_materia.bless(models.Publisher)(TestAuthor)  # Different ORM class

    def test_different_transmuters_same_orm(self):
        """Test that different transmuter classes can be blessed with the same ORM."""
        from arcanum.base import BaseTransmuter

        test_materia = SqlalchemyMateria()

        @test_materia.bless(models.Author)
        class AuthorV1(BaseTransmuter):
            id: int | None = None
            name: str
            field: str

        # This should work - different transmuter class, same ORM
        @test_materia.bless(models.Author)
        class AuthorV2(BaseTransmuter):
            id: int | None = None
            name: str
            field: str

        with test_materia:
            # Both should work independently
            author1 = AuthorV1(name="Author 1", field="Physics")
            author2 = AuthorV2(name="Author 2", field="Biology")

            assert isinstance(author1.__transmuter_provided__, models.Author)
            assert isinstance(author2.__transmuter_provided__, models.Author)

    def test_unblessed_transmuter_no_orm(self):
        """Test that unblessed transmuters don't have ORM objects."""
        from arcanum.base import BaseTransmuter

        class UnblessedTransmuter(BaseTransmuter):
            name: str

        obj = UnblessedTransmuter(name="test")
        assert obj.__transmuter_provided__ is None


class TestORMAttributeAccess:
    """Test accessing ORM attributes through transmuters."""

    def test_transmuter_provides_orm_attributes(self):
        """Test that transmuter fields map to ORM attributes."""
        author = Author(name="Test", field="Physics")

        assert author.name == author.__transmuter_provided__.name  # type: ignore
        assert author.field == author.__transmuter_provided__.field  # type: ignore

    def test_modify_transmuter_updates_orm(self):
        """Test that modifying transmuter fields updates ORM object."""
        author = Author(name="Original", field="Physics")

        # Modify through transmuter
        author.name = "Updated"

        # ORM should reflect change
        assert author.__transmuter_provided__.name == "Updated"  # type: ignore

    def test_modify_orm_visible_in_transmuter(self):
        """Test that modifying ORM attributes is visible through transmuter after revalidate."""
        author = Author(name="Original", field="Physics")

        # Modify ORM directly
        author.__transmuter_provided__.name = "Changed"  # type: ignore

        # Need to revalidate to sync from ORM
        author.revalidate()

        # Should be visible through transmuter
        assert author.name == "Changed"

    def test_orm_and_transmuter_stay_in_sync(self, engine: Engine):
        """Test that ORM and transmuter stay synchronized during persistence."""
        with Session(engine) as session:
            author = Author(name="Sync Test", field="Biology")
            session.add(author)

            # Modify before flush
            author.name = "Updated Before Flush"
            session.flush()

            assert author.__transmuter_provided__.name == "Updated Before Flush"  # type: ignore

            # Modify after flush
            author.name = "Updated After Flush"
            session.flush()

            assert author.__transmuter_provided__.name == "Updated After Flush"  # type: ignore
