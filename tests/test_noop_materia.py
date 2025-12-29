from __future__ import annotations

from typing import Annotated, Optional

import pytest
from pydantic import Field

from arcanum.association import Relation, RelationCollection
from arcanum.base import BaseTransmuter, Identity
from arcanum.materia.base import NoOpMateria


class Author(BaseTransmuter):
    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    books: RelationCollection[Book] = RelationCollection()


class Book(BaseTransmuter):
    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    title: str
    isbn: str
    author_id: int | None = None
    author: Relation[Author] = Relation()


@pytest.fixture(scope="module", autouse=True)
def noop_materia():
    with NoOpMateria():
        yield


def test_noop_materia_works_as_normal_pydantic_model():
    """Test that transmuters without a provider work as normal Pydantic models"""
    author = Author(name="J.K. Rowling")

    # The __provided__ should be None for NoOpMateria
    assert author.__transmuter_provided__ is None

    # Normal attributes should be accessible directly on the model
    assert author.name == "J.K. Rowling"

    # Update attribute and verify it works
    author.name = "Joanne Rowling"
    assert author.name == "Joanne Rowling"


def test_noop_materia_relation_works_like_normal_attribute():
    """Test that Relation works like a normal attribute"""
    author = Author(id=1, name="George Orwell")
    book = Book(id=1, title="1984", isbn="978-0451524935", author_id=1)

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
    book = Book(id=1, title="Animal Farm", isbn="978-0451526342", author_id=None)

    # Initially None
    assert book.author.value is None

    # Set to an author
    author = Author(id=2, name="George Orwell")
    book.author.value = author
    assert book.author.value is not None

    # Set back to None
    book.author.value = None
    assert book.author.value is None


def test_noop_materia_relation_collection_works_like_list():
    """Test that RelationCollection works like a normal list"""
    author = Author(id=1, name="Isaac Asimov")

    # Initially empty
    assert len(author.books) == 0
    assert isinstance(author.books, RelationCollection)

    # Create books
    book1 = Book(id=1, title="Foundation", isbn="978-0553293357", author_id=1)
    book2 = Book(id=2, title="I, Robot", isbn="978-0553382563", author_id=1)
    book3 = Book(id=3, title="The Gods Themselves", isbn="978-0553288100", author_id=1)

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
    author = Author(id=1, name="Frank Herbert")

    book1 = Book(id=1, title="Dune", isbn="978-0441172719", author_id=1)
    book2 = Book(id=2, title="Dune Messiah", isbn="978-0441172696", author_id=1)
    book3 = Book(id=3, title="Children of Dune", isbn="978-0441104024", author_id=1)

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
    author = Author(id=1, name="J.R.R. Tolkien")

    books = [
        Book(id=i, title=f"Book {i}", isbn=f"ISBN-{i}", author_id=1)
        for i in range(1, 6)
    ]
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
        Book(id=10, title="New Book 1", isbn="ISBN-10", author_id=1),
        Book(id=11, title="New Book 2", isbn="ISBN-11", author_id=1),
    ]
    author.books[1:3] = new_books
    assert len(author.books) == 5
    assert author.books[1].title == "New Book 1"
    assert author.books[2].title == "New Book 2"


def test_noop_materia_mixed_attributes():
    """Test mixing normal attributes with relations"""
    author = Author(id=1, name="Arthur C. Clarke")
    book = Book(id=1, title="2001: A Space Odyssey", isbn="978-0451457998", author_id=1)

    # Set book's author relation
    book.author.value = author

    # Add book to author's collection
    author.books.append(book)

    # Verify normal attributes
    assert author.name == "Arthur C. Clarke"
    assert book.title == "2001: A Space Odyssey"
    assert book.isbn == "978-0451457998"

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
        books=RelationCollection(
            [
                Book(id=1, title="Book 1", isbn="ISBN-1", author_id=1),
                Book(id=2, title="Book 2", isbn="ISBN-2", author_id=1),
            ]
        ),
    )

    # Should have the books from initialization
    assert len(author.books) == 2
    assert author.books[0].title == "Book 1"
    assert author.books[1].title == "Book 2"
