from __future__ import annotations

import pytest
from pydantic import ValidationError

from arcanum.association import Relation, RelationCollection
from tests.schemas import Author, Book


def test_noop_materia_works_as_normal_pydantic_model():
    """Test that transmuters without a provider work as normal Pydantic models"""
    author = Author(name="J.K. Rowling", field="Literature")

    # The __provided__ should be None for NoOpMateria
    assert author.__transmuter_provided__ is None

    # Normal attributes should be accessible directly on the model
    assert author.name == "J.K. Rowling"

    # Update attribute and verify it works
    author.name = "Joanne Rowling"
    assert author.name == "Joanne Rowling"


def test_noop_materia_relation_works_like_normal_attribute():
    """Test that Relation works like a normal attribute"""
    author = Author(id=1, name="George Orwell", field="Literature")
    book = Book(id=1, title="1984", year=1949)

    # Set relation value
    book.author.value = author

    # Get relation value
    assert book.author.value is not None
    assert book.author.value.name == "George Orwell"
    assert book.author.value.id == 1

    # Relation should behave like a normal attribute assignment/access
    assert isinstance(book.author, Relation)
    assert isinstance(book.author.value, Author)


def test_noop_materia_relation_set_to_none():
    """Test that Relation can be set to None"""
    book = Book(id=1, title="Animal Farm", year=1945)

    # Initially None
    assert book.author.value is None

    # Set to an author
    author = Author(id=2, name="George Orwell", field="Literature")
    book.author.value = author
    assert book.author.value is not None

    # Author is not allowed to be None
    with pytest.raises(ValidationError):
        book.author.value = None

    book.author.value = Author(id=3, name="Another George Orwell", field="Literature")
    assert book.author.value.name == "Another George Orwell"


def test_noop_materia_relation_collection_works_like_list():
    """Test that RelationCollection works like a normal list"""
    author = Author(id=1, name="Isaac Asimov", field="Chemistry")

    # Initially empty
    assert len(author.books) == 0
    assert isinstance(author.books, RelationCollection)

    # Create books
    book1 = Book(id=1, title="Foundation", year=1951)
    book2 = Book(id=2, title="I, Robot", year=1950)
    book3 = Book(id=3, title="The Gods Themselves", year=1972)

    # Append like a normal list
    author.books.append(book1)
    assert len(author.books) == 1

    # Extend like a normal list
    author.books.extend([book2, book3])
    assert len(author.books) == 3

    # Access by index
    assert author.books[0].title == "Foundation"
    assert author.books[1].title == "I, Robot"
    assert author.books[2].title == "The Gods Themselves"

    # Iterate like a list
    titles = [book.title for book in author.books]
    assert titles == ["Foundation", "I, Robot", "The Gods Themselves"]

    # Contains check
    assert book1 in author.books

    # Boolean check
    assert bool(author.books) is True


def test_noop_materia_relation_collection_list_operations():
    """Test more list operations on RelationCollection"""
    author = Author(id=1, name="Frank Herbert", field="Literature")

    book1 = Book(id=1, title="Dune", year=1965)
    book2 = Book(id=2, title="Dune Messiah", year=1969)
    book3 = Book(id=3, title="Children of Dune", year=1976)

    author.books.extend([book1, book2, book3])

    # Pop
    popped = author.books.pop()
    assert popped.title == "Children of Dune"
    assert len(author.books) == 2

    # Remove
    author.books.remove(book1)
    assert len(author.books) == 1
    assert book1 not in author.books

    # Insert
    author.books.insert(0, book3)
    assert author.books[0].title == "Children of Dune"
    assert len(author.books) == 2

    # Clear
    author.books.clear()
    assert len(author.books) == 0
    assert bool(author.books) is False


def test_noop_materia_relation_collection_slicing():
    """Test slicing operations on RelationCollection"""
    author = Author(id=1, name="J.R.R. Tolkien", field="Literature")

    books = [Book(id=i, title=f"Book {i}", year=1950 + i) for i in range(1, 6)]
    author.books.extend(books)

    # Get slice
    first_three = author.books[:3]
    assert len(first_three) == 3
    assert first_three[0].title == "Book 1"
    assert first_three[2].title == "Book 3"

    # Get slice with step
    odd_indexed = author.books[::2]
    assert len(odd_indexed) == 3
    assert odd_indexed[0].title == "Book 1"
    assert odd_indexed[1].title == "Book 3"
    assert odd_indexed[2].title == "Book 5"

    # Set slice
    new_books = [
        Book(id=10, title="New Book 1", year=1960),
        Book(id=11, title="New Book 2", year=1961),
    ]
    author.books[1:3] = new_books
    assert len(author.books) == 5
    assert author.books[1].title == "New Book 1"
    assert author.books[2].title == "New Book 2"


def test_noop_materia_mixed_attributes():
    """Test mixing normal attributes with relations"""
    author = Author(id=1, name="Arthur C. Clarke", field="Physics")
    book = Book(id=1, title="2001: A Space Odyssey", year=1968)

    # Set book's author relation
    book.author.value = author

    # Add book to author's collection
    author.books.append(book)

    # Verify normal attributes
    assert author.name == "Arthur C. Clarke"
    assert book.title == "2001: A Space Odyssey"
    assert book.year == 1968

    # Verify relations
    assert book.author.value.name == "Arthur C. Clarke"
    assert len(author.books) == 1
    assert author.books[0].title == "2001: A Space Odyssey"

    # Update normal attributes
    author.name = "Sir Arthur C. Clarke"
    book.title = "2001: A Space Odyssey (50th Anniversary Edition)"

    # Verify updates are reflected
    assert author.name == "Sir Arthur C. Clarke"
    assert book.title == "2001: A Space Odyssey (50th Anniversary Edition)"
    assert book.author.value.name == "Sir Arthur C. Clarke"


def test_noop_materia_relation_collection_initialization():
    """Test RelationCollection initialization with data"""
    author = Author(
        id=1,
        name="Test Author",
        field="History",
        books=RelationCollection(
            [
                Book(id=1, title="Book 1", year=2000),
                Book(id=2, title="Book 2", year=2001),
            ]
        ),
    )

    # Should have the books from initialization
    assert len(author.books) == 2
    assert author.books[0].title == "Book 1"
    assert author.books[1].title == "Book 2"
