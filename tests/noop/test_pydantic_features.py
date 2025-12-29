from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from tests.schemas import Author, Book, Publisher, Review

"""Test basic Pydantic features and transmuter fundamentals with NoOpMateria.

This module tests:
- Basic model instantiation and attribute access
- Pydantic validation behavior
- Serialization (dict, JSON)
- Model copying and updating
- Field defaults and frozen fields
- Transmuter-specific attributes (__transmuter_provided__)
"""


# Regular Pydantic BaseModel for testing nested compatibility
class Address(BaseModel):
    """A regular Pydantic model to test nesting in Transmuters."""

    street: str
    city: str
    country: str
    zip_code: str | None = None


class Contact(BaseModel):
    """Another regular Pydantic model."""

    email: str
    phone: str | None = None
    address: Address | None = None


class TestBasicModelBehavior:
    """Test that transmuters work as normal Pydantic models."""

    def test_model_instantiation(self):
        """Test basic model creation with valid data."""
        author = Author(name="J.K. Rowling", field="Literature")

        assert author.name == "J.K. Rowling"
        assert author.field == "Literature"
        assert author.id is None

    def test_model_with_all_fields(self):
        """Test model creation with all fields including optional ones."""
        book = Book(
            id=1,
            title="Harry Potter",
            year=1997,
            author_id=1,
            publisher_id=2,
        )

        assert book.id == 1
        assert book.title == "Harry Potter"
        assert book.year == 1997
        assert book.author_id == 1
        assert book.publisher_id == 2

    def test_attribute_access_and_mutation(self):
        """Test getting and setting attributes."""
        author = Author(name="J.K. Rowling", field="Literature")

        # Read attribute
        assert author.name == "J.K. Rowling"

        # Update attribute
        author.name = "Joanne Rowling"
        assert author.name == "Joanne Rowling"

        # Update another attribute
        author.field = "History"
        assert author.field == "History"

    def test_transmuter_provided_is_none_with_noop(self):
        """Test that __transmuter_provided__ is None when using NoOpMateria."""
        author = Author(name="Isaac Asimov", field="Chemistry")
        assert author.__transmuter_provided__ is None

        book = Book(id=1, title="Foundation", year=1951)
        assert book.__transmuter_provided__ is None


class TestPydanticValidation:
    """Test Pydantic validation features."""

    def test_field_validation(self):
        """Test that field validation works correctly."""
        # Valid field value
        author = Author(name="George Orwell", field="Literature")
        assert author.field == "Literature"

        # Invalid field value should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Author(name="Test Author", field="InvalidField")  # pyright: ignore[reportArgumentType]

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "field" in errors[0]["loc"]
        assert "literal_error" in errors[0]["type"]

    def test_required_fields(self):
        """Test that required fields must be provided."""
        # Missing required field 'name'
        with pytest.raises(ValidationError) as exc_info:
            Author(field="Physics")  # pyright: ignore[reportCallIssue]

        errors = exc_info.value.errors()
        assert any("name" in str(error["loc"]) for error in errors)

        # Missing required field 'title'
        with pytest.raises(ValidationError) as exc_info:
            Book(year=2000)  # pyright: ignore[reportCallIssue]

        errors = exc_info.value.errors()
        assert any("title" in str(error["loc"]) for error in errors)

    def test_type_validation(self):
        """Test that field types are validated."""
        # Invalid type for year (should be int)
        with pytest.raises(ValidationError) as exc_info:
            Book(title="Test Book", year="not a number")  # pyright: ignore[reportArgumentType]

        errors = exc_info.value.errors()
        assert any("year" in str(error["loc"]) for error in errors)

    def test_frozen_field_identity(self):
        """Test that Identity-marked fields are frozen after creation."""
        author = Author(id=1, name="Arthur C. Clarke", field="Physics")

        # ID is frozen, cannot be changed
        with pytest.raises(ValidationError):
            author.id = 2

        # Other fields can be changed
        author.name = "Sir Arthur C. Clarke"
        assert author.name == "Sir Arthur C. Clarke"


class TestSerialization:
    """Test model serialization features."""

    def test_model_dump(self):
        """Test model_dump() returns correct dictionary."""
        author = Author(id=1, name="Isaac Asimov", field="Chemistry")
        data = author.model_dump()

        assert data == {
            "id": 1,
            "name": "Isaac Asimov",
            "field": "Chemistry",
            "books": [],
        }

    def test_model_dump_all_fields(self):
        """Test serialization with all fields including foreign keys."""
        book = Book(
            id=1,
            title="Foundation",
            year=1951,
            author_id=10,
            publisher_id=20,
        )
        data = book.model_dump()

        assert data["id"] == 1
        assert data["title"] == "Foundation"
        assert data["year"] == 1951
        assert data["author_id"] == 10
        assert data["publisher_id"] == 20

    def test_model_dump_with_nested_associations(self):
        """Test serialization with nested models including associations."""
        # Create author with nested books
        author = Author(id=1, name="Isaac Asimov", field="Chemistry")
        book1 = Book(id=10, title="Foundation", year=1951)
        book2 = Book(id=11, title="I, Robot", year=1950)
        author.books.extend([book1, book2])

        data = author.model_dump()

        # Check author fields
        assert data["id"] == 1
        assert data["name"] == "Isaac Asimov"
        assert data["field"] == "Chemistry"

        # Check nested books are serialized
        assert len(data["books"]) == 2
        assert data["books"][0]["title"] == "Foundation"
        assert data["books"][0]["year"] == 1951
        assert data["books"][1]["title"] == "I, Robot"
        assert data["books"][1]["year"] == 1950

    def test_model_dump_with_relation_value(self):
        """Test serialization with Relation field containing a value."""
        author = Author(id=1, name="George Orwell", field="Literature")
        publisher = Publisher(id=2, name="Secker & Warburg", country="United Kingdom")

        book = Book(id=10, title="1984", year=1949)
        book.author.value = author
        book.publisher.value = publisher

        data = book.model_dump()

        # Check book fields
        assert data["id"] == 10
        assert data["title"] == "1984"
        assert data["year"] == 1949

        # Check nested author is serialized
        assert data["author"]["id"] == 1
        assert data["author"]["name"] == "George Orwell"
        assert data["author"]["field"] == "Literature"

        # Check nested publisher is serialized
        assert data["publisher"]["id"] == 2
        assert data["publisher"]["name"] == "Secker & Warburg"
        assert data["publisher"]["country"] == "United Kingdom"

    def test_model_validate_with_nested_dict(self):
        """Test creating models from nested dictionaries (standard Pydantic feature)."""
        # Create author with nested books from dict
        author_data = {
            "id": 1,
            "name": "J.R.R. Tolkien",
            "field": "Literature",
            "books": [
                {"id": 100, "title": "The Hobbit", "year": 1937},
                {"id": 101, "title": "The Fellowship of the Ring", "year": 1954},
            ],
        }

        author = Author.model_validate(author_data)

        # Verify author fields
        assert author.id == 1
        assert author.name == "J.R.R. Tolkien"
        assert author.field == "Literature"

        # Verify nested books were created
        assert len(author.books) == 2
        assert isinstance(author.books[0], Book)
        assert author.books[0].title == "The Hobbit"
        assert author.books[0].year == 1937
        assert isinstance(author.books[1], Book)
        assert author.books[1].title == "The Fellowship of the Ring"
        assert author.books[1].year == 1954

    def test_nested_model_validation(self):
        """Test that nested models are validated correctly."""
        # Valid nested data
        book_data = {
            "id": 1,
            "title": "1984",
            "year": 1949,
            "author": {
                "id": 10,
                "name": "George Orwell",
                "field": "Literature",
            },
        }

        book = Book.model_validate(book_data)
        assert book.title == "1984"
        assert book.author.value is not None
        assert book.author.value.name == "George Orwell"

        # Invalid nested data - should raise ValidationError
        invalid_book_data = {
            "id": 1,
            "title": "Test",
            "year": 2000,
            "author": {
                "name": "Test Author",
                "field": "InvalidField",  # Invalid literal value
            },
        }

        with pytest.raises(ValidationError) as exc_info:
            Book.model_validate(invalid_book_data)

        errors = exc_info.value.errors()
        # Should have error in nested author field
        assert any("author" in str(error["loc"]) for error in errors)

    def test_model_dump_json(self):
        """Test model_dump_json() returns valid JSON string."""
        author = Author(id=1, name="Frank Herbert", field="Literature")
        json_str = author.model_dump_json()

        assert isinstance(json_str, str)
        assert (
            '"name":"Frank Herbert"' in json_str
            or '"name": "Frank Herbert"' in json_str
        )

    def test_model_dump_exclude(self):
        """Test model_dump with exclude parameter."""
        author = Author(id=1, name="Ursula K. Le Guin", field="Literature")
        data = author.model_dump(exclude={"id"})

        assert "id" not in data
        assert data["name"] == "Ursula K. Le Guin"
        assert data["field"] == "Literature"

    def test_model_dump_include(self):
        """Test model_dump with include parameter."""
        author = Author(id=1, name="Ray Bradbury", field="Literature")
        data = author.model_dump(include={"name", "field"})

        assert len(data) == 2
        assert data["name"] == "Ray Bradbury"
        assert data["field"] == "Literature"

    def test_model_dump_exclude_relationships(self):
        """Test model_dump excluding relationship fields."""
        # Create author with books
        author = Author(id=1, name="Terry Pratchett", field="Literature")
        book1 = Book(id=10, title="Guards! Guards!", year=1989)
        book2 = Book(id=11, title="Small Gods", year=1992)
        author.books.extend([book1, book2])

        # Exclude the books relationship
        data = author.model_dump(exclude={"books"})

        assert "books" not in data
        assert data["id"] == 1
        assert data["name"] == "Terry Pratchett"
        assert data["field"] == "Literature"

    def test_model_dump_include_relationships(self):
        """Test model_dump including only relationship fields."""
        # Create author with books
        author = Author(id=1, name="Neil Gaiman", field="Literature")
        book1 = Book(id=10, title="American Gods", year=2001)
        book2 = Book(id=11, title="Neverwhere", year=1996)
        author.books.extend([book1, book2])

        # Include only name and books
        data = author.model_dump(include={"name", "books"})

        assert len(data) == 2
        assert data["name"] == "Neil Gaiman"
        assert len(data["books"]) == 2
        assert data["books"][0]["title"] == "American Gods"
        assert data["books"][1]["title"] == "Neverwhere"
        # id and field should not be included
        assert "id" not in data
        assert "field" not in data

    def test_model_dump_exclude_nested_relationship_fields(self):
        """Test model_dump with exclude on nested relationship data."""
        # Create book with author relation
        author = Author(id=1, name="Douglas Adams", field="Literature")
        book = Book(id=42, title="The Hitchhiker's Guide to the Galaxy", year=1979)
        book.author.value = author
        book.reviews.extend(
            [
                Review(
                    id=1,
                    reviewer_name="Reviewer1",
                    rating=5,
                    comment="Excellent read!",
                ),
                Review(
                    id=2,
                    reviewer_name="Reviewer2",
                    rating=4,
                    comment="Enjoyed it.",
                ),
            ]
        )

        # author.books.append(book)

        # Exclude specific fields from the nested author
        data = book.model_dump(
            exclude={"author": {"field", "books"}, "reviews": {"__all__": {"id"}}}
        )

        assert data["id"] == 42
        assert data["title"] == "The Hitchhiker's Guide to the Galaxy"
        assert data["author"]["id"] == 1
        assert data["author"]["name"] == "Douglas Adams"

        pass
        # These should be excluded from nested author
        assert "field" not in data["author"]
        assert "books" not in data["author"]

    def test_model_dump_include_nested_relationship_fields(self):
        """Test model_dump with include on nested relationship data."""
        # Create book with author and publisher
        author = Author(id=1, name="Terry Pratchett", field="Literature")
        publisher = Publisher(id=2, name="Victor Gollancz", country="UK")
        book = Book(id=10, title="The Colour of Magic", year=1983)
        book.author.value = author
        book.publisher.value = publisher

        # Include only specific nested fields
        data = book.model_dump(include={"title": True, "author": {"name"}})

        assert data["title"] == "The Colour of Magic"
        assert data["author"]["name"] == "Terry Pratchett"
        # Only name should be in author
        assert len(data["author"]) == 1
        # Other fields should not be included
        assert "id" not in data
        assert "year" not in data
        assert "publisher" not in data

    def test_model_dump_exclude_all_relationships(self):
        """Test model_dump excluding all relationship fields from a model."""
        # Create book with relations
        author = Author(id=1, name="Test Author", field="Literature")
        publisher = Publisher(id=2, name="Test Publisher", country="USA")
        book = Book(
            id=10,
            title="Test Book",
            year=2000,
            author_id=1,
            publisher_id=2,
        )
        book.author.value = author
        book.publisher.value = publisher

        # Exclude all relationship fields
        data = book.model_dump(exclude={"author", "publisher"})

        # Only scalar fields should remain
        assert data["id"] == 10
        assert data["title"] == "Test Book"
        assert data["year"] == 2000
        assert data["author_id"] == 1
        assert data["publisher_id"] == 2
        assert "author" not in data
        assert "publisher" not in data


class TestBaseModelNesting:
    """Test that regular Pydantic BaseModel can be nested in Transmuters."""

    def test_transmuter_with_basemodel_field(self):
        """Test that a Transmuter can have a regular BaseModel as a field."""
        from typing import Annotated

        from pydantic import Field

        from arcanum.base import BaseTransmuter, Identity

        # Create a Transmuter with BaseModel fields
        class PublisherWithContact(BaseTransmuter):
            id: Annotated[int | None, Identity] = Field(default=None, frozen=True)
            name: str
            country: str
            contact: Contact | None = None

        # Create publisher with nested BaseModel
        address = Address(
            street="123 Main St", city="New York", country="USA", zip_code="10001"
        )
        contact = Contact(
            email="info@publisher.com", phone="+1-555-0100", address=address
        )
        publisher = PublisherWithContact(
            id=1, name="Example Press", country="USA", contact=contact
        )

        # Verify fields
        assert publisher.id == 1
        assert publisher.name == "Example Press"
        assert publisher.contact is not None
        assert publisher.contact.email == "info@publisher.com"
        assert publisher.contact.address is not None
        assert publisher.contact.address.city == "New York"

    def test_transmuter_serialize_with_basemodel(self):
        """Test serialization of Transmuter with nested BaseModel."""
        from typing import Annotated

        from pydantic import Field

        from arcanum.base import BaseTransmuter, Identity

        class AuthorWithContact(BaseTransmuter):
            id: Annotated[int | None, Identity] = Field(default=None, frozen=True)
            name: str
            field: str
            contact: Contact | None = None

        address = Address(street="456 Oak Ave", city="London", country="UK")
        contact = Contact(email="author@example.com", address=address)
        author = AuthorWithContact(
            id=1, name="Test Author", field="Literature", contact=contact
        )

        # Serialize to dict
        data = author.model_dump()

        assert data["id"] == 1
        assert data["name"] == "Test Author"
        assert data["contact"]["email"] == "author@example.com"
        assert data["contact"]["address"]["city"] == "London"
        assert data["contact"]["address"]["country"] == "UK"

    def test_transmuter_validate_with_basemodel(self):
        """Test validation of Transmuter with nested BaseModel from dict."""
        from typing import Annotated

        from pydantic import Field

        from arcanum.base import BaseTransmuter, Identity

        class PublisherWithAddress(BaseTransmuter):
            id: Annotated[int | None, Identity] = Field(default=None, frozen=True)
            name: str
            country: str
            address: Address | None = None

        # Create from dict
        publisher_data = {
            "id": 1,
            "name": "Oxford Press",
            "country": "UK",
            "address": {
                "street": "Oxford Street",
                "city": "Oxford",
                "country": "UK",
                "zip_code": "OX1 1XX",
            },
        }

        publisher = PublisherWithAddress.model_validate(publisher_data)

        assert publisher.name == "Oxford Press"
        assert publisher.address is not None
        assert isinstance(publisher.address, Address)
        assert publisher.address.street == "Oxford Street"
        assert publisher.address.zip_code == "OX1 1XX"

    def test_basemodel_validation_in_transmuter(self):
        """Test that validation works for nested BaseModel in Transmuter."""
        from typing import Annotated

        from pydantic import Field, field_validator

        from arcanum.base import BaseTransmuter, Identity

        class ContactInfo(BaseModel):
            email: str

            @field_validator("email")
            @classmethod
            def validate_email(cls, v: str) -> str:
                if "@" not in v:
                    raise ValueError("Invalid email format")
                return v

        class AuthorWithValidation(BaseTransmuter):
            id: Annotated[int | None, Identity] = Field(default=None, frozen=True)
            name: str
            field: str
            contact_info: ContactInfo | None = None

        # Valid email
        author = AuthorWithValidation(
            name="Test",
            field="Physics",
            contact_info=ContactInfo(email="test@example.com"),
        )
        assert author.contact_info
        assert author.contact_info.email == "test@example.com"

        # Invalid email should raise error
        with pytest.raises(ValidationError):
            AuthorWithValidation(
                name="Test",
                field="Physics",
                contact_info=ContactInfo(email="invalid-email"),
            )


class TestModelCopyAndUpdate:
    """Test model copying and updating operations."""

    def test_model_copy(self):
        """Test model_copy() creates a new instance."""
        original = Author(id=1, name="Philip K. Dick", field="Literature")
        copied = original.model_copy()

        assert copied is not original
        assert copied.id == original.id
        assert copied.name == original.name
        assert copied.field == original.field

    def test_model_copy_update(self):
        """Test model_copy(update=...) creates modified copy."""
        original = Author(id=1, name="William Gibson", field="Literature")
        updated = original.model_copy(update={"name": "Bruce Sterling"})

        # Original unchanged
        assert original.name == "William Gibson"

        # Copy has updated value
        assert updated.name == "Bruce Sterling"
        assert updated.id == original.id
        assert updated.field == original.field

    def test_model_copy_deep(self):
        """Test deep copy behavior."""
        original = Author(id=1, name="Neal Stephenson", field="Literature")
        copied = original.model_copy(deep=True)

        assert copied is not original
        assert copied.name == original.name

        # Modify copy shouldn't affect original
        copied.name = "Modified Name"
        assert original.name == "Neal Stephenson"
        assert copied.name == "Modified Name"


class TestFieldDefaults:
    """Test field default values and factories."""

    def test_optional_fields_default_to_none(self):
        """Test that optional fields have None as default."""
        author = Author(name="Test Author", field="Physics")

        assert author.id is None

    def test_collection_fields_default_to_empty(self):
        """Test that collection fields default to empty collections."""
        author = Author(name="Test Author", field="Literature")

        assert len(author.books) == 0
        assert isinstance(author.books, list)

    def test_explicit_none_for_optional_fields(self):
        """Test explicitly setting None for optional fields."""
        book = Book(id=None, title="Test Book", year=2000)

        assert book.id is None

    def test_partial_model_creation(self):
        """Test creating models with only required fields."""
        # Only required fields
        publisher = Publisher(name="Test Publisher", country="USA")

        assert publisher.name == "Test Publisher"
        assert publisher.country == "USA"
        assert publisher.id is None
        assert len(publisher.books) == 0


class TestModelComparison:
    """Test model equality and comparison operations."""

    def test_model_equality(self):
        """Test that models with same data are equal."""
        author1 = Author(id=1, name="Test Author", field="Physics")
        author2 = Author(id=1, name="Test Author", field="Physics")

        # Pydantic models are equal if their fields are equal
        assert author1 == author2

    def test_model_inequality_different_values(self):
        """Test that models with different data are not equal."""
        author1 = Author(id=1, name="Author One", field="Physics")
        author2 = Author(id=2, name="Author Two", field="Biology")

        assert author1 != author2

    def test_model_inequality_different_types(self):
        """Test that different model types are not equal."""
        author = Author(id=1, name="Test", field="Physics")
        publisher = Publisher(id=1, name="Test", country="USA")

        assert author != publisher
