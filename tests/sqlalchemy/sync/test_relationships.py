"""Test relationship handling.

Tests:
- 1-1 relationships (Book <-> BookDetail, Book <-> Translator)
- 1-M relationships (Author <-> Books, Publisher <-> Books)
- M-M relationships (Book <-> Categories)
- Circular relationships (Author -> Book -> Author)
- Lazy loading behaviors
- Eager loading with selectinload/joinedload
- lazy='raise' and raiseload() preventing N+1
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import joinedload, raiseload, selectinload

from arcanum.materia.sqlalchemy import Session
from tests import models
from tests.schemas import (
    Author,
    Book,
    BookDetail,
    Category,
    Publisher,
    Review,
    Translator,
)


class TestOneToOneRelationships:
    """Test 1-1 relationship handling."""

    def test_book_to_book_detail_forward(self, engine: Engine):
        """Test accessing BookDetail from Book (1-1 forward)."""
        with Session(engine) as session:
            author = Author(name="1-1 Author", field="Physics")
            publisher = Publisher(name="1-1 Publisher", country="USA")
            book = Book(title="1-1 Test Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-1234567890",
                pages=300,
                abstract="Test abstract",
            )
            book.detail.value = detail

            session.add(book)
            session.flush()

            # Access detail through book
            assert book.detail.value is not None
            assert book.detail.value.isbn == "978-1234567890"
            assert book.detail.value.pages == 300

    def test_book_detail_to_book_backward(self, engine: Engine):
        """Test accessing Book from BookDetail (1-1 backward)."""
        with Session(engine) as session:
            author = Author(name="1-1 Back Author", field="Biology")
            publisher = Publisher(name="1-1 Back Pub", country="UK")
            book = Book(title="1-1 Back Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-0987654321",
                pages=250,
                abstract="Back reference test",
            )
            book.detail.value = detail

            session.add(book)
            session.flush()

            # Access book through detail
            assert detail.book.value is not None
            assert detail.book.value.title == "1-1 Back Book"

    def test_optional_one_to_one_translator(self, engine: Engine):
        """Test optional 1-1 relationship with Translator."""
        with Session(engine) as session:
            # Book without translator
            author = Author(name="No Trans Author", field="Literature")
            publisher = Publisher(name="No Trans Pub", country="USA")
            book1 = Book(title="Original Language", year=2024)
            book1.author.value = author
            book1.publisher.value = publisher

            # Book with translator
            translator = Translator(name="John Translator", language="Spanish")
            book2 = Book(title="Translated Book", year=2024)
            book2.author.value = author
            book2.publisher.value = publisher
            book2.translator.value = translator

            session.add_all([book1, book2])
            session.flush()

            # Book1 has no translator
            assert book1.translator.value is None

            # Book2 has translator
            assert book2.translator.value is not None
            assert book2.translator.value.name == "John Translator"

    def test_one_to_one_bidirectional(self, engine: Engine):
        """Test bidirectional 1-1 relationship consistency."""
        with Session(engine) as session:
            author = Author(name="Bidir Author", field="Chemistry")
            publisher = Publisher(name="Bidir Pub", country="USA")
            book = Book(title="Bidir Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-1111111111",
                pages=200,
                abstract="Bidirectional test",
            )
            book.detail.value = detail

            session.add(book)
            session.flush()

            # Both directions should work
            assert book.detail.value is detail
            assert detail.book.value is book


class TestOneToManyRelationships:
    """Test 1-M relationship handling."""

    def test_author_to_books_forward(self, engine: Engine):
        """Test accessing Books from Author (1-M forward)."""
        with Session(engine) as session:
            author = Author(name="Prolific Author", field="Literature")
            publisher = Publisher(name="1-M Publisher", country="USA")

            book = Book(title="A Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            session.add(author)
            # session.add(publisher)
            session.flush()
            author.revalidate()

            # Access books through author
            assert len(author.books) == 1
            assert all(isinstance(b, Book) for b in author.books)

    def test_book_to_author_backward(self, engine: Engine):
        """Test accessing Author from Book (M-1 backward)."""
        with Session(engine) as session:
            author = Author(name="M-1 Author", field="Physics")
            publisher = Publisher(name="M-1 Pub", country="UK")
            book = Book(title="M-1 Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            session.add(book)
            session.flush()

            # Access author through book
            assert book.author.value is not None
            assert book.author.value.name == "M-1 Author"

    def test_publisher_to_books(self, engine: Engine):
        """Test Publisher to Books (1-M)."""
        with Session(engine) as session:
            publisher = Publisher(name="Big Publisher", country="USA")
            author = Author(name="Pub Author", field="Biology")

            for i in range(5):
                book = Book(title=f"Published Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher

            session.add(publisher)
            session.flush()
            publisher.revalidate()

            assert len(publisher.books) == 5

    def test_one_to_many_append_and_remove(self, engine: Engine):
        """Test appending and removing items from 1-M relationship."""
        with Session(engine) as session:
            author = Author(name="Modify Author", field="Chemistry")
            publisher = Publisher(name="Modify Pub", country="USA")

            book1 = Book(title="First Book", year=2023)
            book1.author.value = author
            book1.publisher.value = publisher

            book2 = Book(title="Second Book", year=2024)
            book2.author.value = author
            book2.publisher.value = publisher

            # SQLAlchemy backref handles the append automatically

            session.add(author)
            session.flush()
            author.revalidate()

            assert len(author.books) == 2

            # Remove one
            author.books.remove(book1)
            session.flush()
            author.revalidate()

            assert len(author.books) == 1
            assert author.books[0]["title"] == "Second Book"


class TestManyToManyRelationships:
    """Test M-M relationship handling."""

    def test_book_to_categories_forward(self, engine: Engine):
        """Test accessing Categories from Book (M-M forward)."""
        with Session(engine) as session:
            author = Author(name="M-M Author", field="Literature")
            publisher = Publisher(name="M-M Pub", country="USA")
            book = Book(title="M-M Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            cat1 = Category(name="M-M Fiction", description="Fiction books")
            cat2 = Category(name="M-M Drama", description="Drama books")
            cat3 = Category(name="M-M Classic", description="Classic literature")

            book.categories.extend([cat1, cat2, cat3])

            session.add(book)
            session.flush()
            book.revalidate()

            # Access categories through book
            assert len(book.categories) == 3
            cat_names = {c["name"] for c in book.categories}
            assert "M-M Fiction" in cat_names
            assert "M-M Drama" in cat_names
            assert "M-M Classic" in cat_names

    def test_category_to_books_backward(self, engine: Engine):
        """Test accessing Books from Category (M-M backward)."""
        with Session(engine) as session:
            category = Category(name="Science Fiction", description="Sci-fi books")
            author = Author(name="Sci-Fi Author", field="Literature")
            publisher = Publisher(name="Sci-Fi Pub", country="USA")

            for i in range(4):
                book = Book(title=f"Sci-Fi Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher
                book.categories.append(category)
                # SQLAlchemy backref handles the other direction

            session.add(category)
            session.flush()
            category.revalidate()

            # Access books through category
            assert len(category.books) == 4

    def test_many_to_many_bidirectional(self, engine: Engine):
        """Test bidirectional M-M relationship."""
        with Session(engine) as session:
            author = Author(name="Bidir M-M Author", field="History")
            publisher = Publisher(name="Bidir M-M Pub", country="UK")
            book = Book(title="Bidir M-M Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            cat = Category(name="Bidir M-M Cat", description="Test category")

            book.categories.append(cat)
            # SQLAlchemy backref handles the other direction

            session.add(book)
            session.flush()
            book.revalidate()
            cat.revalidate()

            # Both directions should work
            assert cat in book.categories
            assert book in cat.books

    def test_many_to_many_remove(self, engine: Engine):
        """Test removing from M-M relationship."""
        with Session(engine) as session:
            author = Author(name="Remove M-M Author", field="Literature")
            publisher = Publisher(name="Remove M-M Pub", country="USA")
            book = Book(title="Remove M-M Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            cat1 = Category(name="Remove Cat 1", description="Cat 1")
            cat2 = Category(name="Remove Cat 2", description="Cat 2")

            book.categories.extend([cat1, cat2])

            session.add(book)
            session.flush()
            book.revalidate()

            assert len(book.categories) == 2

            # Remove one category
            book.categories.remove(cat1)
            session.flush()
            book.revalidate()

            assert len(book.categories) == 1
            assert book.categories[0].name == "Remove Cat 2"


class TestCircularRelationships:
    """Test circular/bidirectional relationships."""

    def test_author_book_circular_reference(self, engine: Engine):
        """Test circular reference: Author -> Book -> Author."""
        with Session(engine) as session:
            author = Author(name="Circular Author", field="Physics")
            publisher = Publisher(name="Circular Pub", country="USA")
            book = Book(title="Circular Book", year=2024)

            # Set up circular reference
            book.author.value = author
            book.publisher.value = publisher
            # SQLAlchemy backref handles author.books

            session.add(author)
            session.flush()
            author.revalidate()
            book.revalidate()

            # Navigate in circle
            assert book.author.value is author
            assert book in author.books
            assert author.books[0].author.value is author

    def test_book_detail_circular(self, engine: Engine):
        """Test circular reference: Book -> BookDetail -> Book."""
        with Session(engine) as session:
            author = Author(name="Detail Circular", field="Biology")
            publisher = Publisher(name="Detail Circular Pub", country="UK")
            book = Book(title="Detail Circular Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-2222222222",
                pages=150,
                abstract="Circular detail",
            )

            book.detail.value = detail
            # SQLAlchemy backref handles detail.book

            session.add(book)
            session.flush()
            book.revalidate()
            detail.revalidate()

            # Navigate in circle
            assert book.detail.value.book.value is book


class TestLazyLoading:
    """Test lazy loading behavior."""

    def test_lazy_load_collection(self, engine: Engine):
        """Test that collections are lazy-loaded by default."""
        with Session(engine) as session:
            author = Author(name="Lazy Author", field="Literature")
            publisher = Publisher(name="Lazy Pub", country="USA")

            for i in range(3):
                book = Book(title=f"Lazy Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher
                # SQLAlchemy backref handles author.books

            session.add(author)
            session.flush()
            author.revalidate()
            author_id = author["id"]

            # Clear session to force reload
            session.expunge_all()

            # Load author without books
            loaded_author = session.get_one(Author, author_id)

            # Accessing books should trigger lazy load
            books = loaded_author.books
            assert len(books) == 3

    def test_lazy_load_single_object(self, engine: Engine):
        """Test lazy loading single object (M-1 relation)."""
        with Session(engine) as session:
            author = Author(name="Lazy Single Author", field="Physics")
            publisher = Publisher(name="Lazy Single Pub", country="USA")
            book = Book(title="Lazy Single Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            session.add(book)
            session.flush()
            book.revalidate()
            book_id = book["id"]

            # Clear session
            session.expunge_all()

            # Load book
            loaded_book = session.get_one(Book, book_id)

            # Accessing author should lazy load
            assert loaded_book.author.value.name == "Lazy Single Author"


class TestEagerLoading:
    """Test eager loading strategies."""

    def test_selectinload_prevents_n_plus_1(self, engine: Engine):
        """Test selectinload prevents N+1 queries."""
        with Session(engine) as session:
            publisher = Publisher(name="Selectin Pub", country="USA")

            # Create multiple authors with books
            for i in range(5):
                author = Author(name=f"Selectin Author {i}", field="Physics")
                book = Book(title=f"Selectin Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher
                # SQLAlchemy backref handles author.books
                session.add(author)

            session.flush()

            # Clear session
            session.expunge_all()

            # Load all books with selectinload for author
            stmt = select(Book).options(selectinload(models.Book.author))
            books = session.execute(stmt).scalars().all()

            assert len(books) == 5

            # Access all authors without additional queries
            for book in books:
                assert book.author.value is not None

    def test_joinedload_loads_in_one_query(self, engine: Engine):
        """Test joinedload loads related objects in one query."""
        with Session(engine) as session:
            author = Author(name="Joined Author", field="Biology")
            publisher = Publisher(name="Joined Pub", country="UK")

            for i in range(3):
                book = Book(title=f"Joined Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher
                author.books.append(book)

            session.add(author)
            session.flush()
            author_id = author.id

            # Clear session
            session.expunge_all()

            # Load author with joined books
            stmt = (
                select(Author)
                .where(Author["id"] == author_id)
                .options(joinedload(models.Author.books))
            )
            loaded_author = session.execute(stmt).scalars().unique().one()

            # Books should be loaded
            assert len(loaded_author.books) == 3

    def test_selectinload_many_to_many(self, engine: Engine):
        """Test selectinload with M-M relationship."""
        with Session(engine) as session:
            author = Author(name="Selectin M-M Author", field="Literature")
            publisher = Publisher(name="Selectin M-M Pub", country="USA")

            for i in range(3):
                book = Book(title=f"Selectin M-M Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher

                cat1 = Category(name=f"Selectin Cat {i}-1", description="Category 1")
                cat2 = Category(name=f"Selectin Cat {i}-2", description="Category 2")
                book.categories.extend([cat1, cat2])

                session.add(book)

            session.flush()

            # Clear session
            session.expunge_all()

            # Load books with categories
            stmt = select(Book).options(selectinload(models.Book.categories))
            books = session.execute(stmt).scalars().all()

            # Categories should be loaded
            for book in books:
                assert len(book.categories) == 2


class TestRaiseOnSQLBehavior:
    """Test lazy='raise' and raiseload() preventing implicit SQL."""

    def test_raiseload_prevents_lazy_loading(self, engine: Engine):
        """Test that raiseload() prevents lazy loading."""
        with Session(engine) as session:
            author = Author(name="Raise Author", field="Chemistry")
            publisher = Publisher(name="Raise Pub", country="USA")
            book = Book(title="Raise Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            session.add(book)
            session.flush()
            book_id = book.id

            # Clear session
            session.expunge_all()

            # Load book with raiseload on author
            stmt = (
                select(Book)
                .where(Book["id"] == book_id)
                .options(raiseload(models.Book.author))
            )
            loaded_book = session.execute(stmt).scalars().one()

            # Accessing author should raise
            with pytest.raises(InvalidRequestError, match="lazy"):
                _ = loaded_book.author.value

    def test_raiseload_collection(self, engine: Engine):
        """Test raiseload on collection relationships."""
        with Session(engine) as session:
            author = Author(name="Raise Collection Author", field="Physics")
            publisher = Publisher(name="Raise Collection Pub", country="USA")

            for i in range(2):
                book = Book(title=f"Raise Collection Book {i}", year=2024)
                book.author.value = author
                book.publisher.value = publisher
                author.books.append(book)

            session.add(author)
            session.flush()
            author_id = author.id

            # Clear session
            session.expunge_all()

            # Load author with raiseload on books
            stmt = (
                select(Author)
                .where(Author["id"] == author_id)
                .options(raiseload(models.Author.books))
            )
            loaded_author = session.execute(stmt).scalars().one()

            # Accessing books should raise
            with pytest.raises(InvalidRequestError, match="lazy"):
                _ = loaded_author.books


class TestReviewsOneToMany:
    """Test Reviews as another 1-M relationship example."""

    def test_book_to_reviews(self, engine: Engine):
        """Test accessing Reviews from Book."""
        with Session(engine) as session:
            author = Author(name="Review Author", field="Literature")
            publisher = Publisher(name="Review Pub", country="USA")
            book = Book(title="Reviewed Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            review1 = Review(
                reviewer_name="Alice",
                rating=5,
                comment="Excellent!",
            )
            review2 = Review(
                reviewer_name="Bob",
                rating=4,
                comment="Good read",
            )

            book.reviews.extend([review1, review2])

            session.add(book)
            session.flush()

            # Access reviews through book
            assert len(book.reviews) == 2
            ratings = [r.rating for r in book.reviews]
            assert 5 in ratings
            assert 4 in ratings

    def test_review_to_book(self, engine: Engine):
        """Test accessing Book from Review."""
        with Session(engine) as session:
            author = Author(name="Review Back Author", field="History")
            publisher = Publisher(name="Review Back Pub", country="UK")
            book = Book(title="Back Review Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            review = Review(
                reviewer_name="Charlie",
                rating=3,
                comment="Okay",
            )

            book.reviews.append(review)

            session.add(book)
            session.flush()

            # Access book through review
            assert review.book.value is not None
            assert review.book.value.title == "Back Review Book"


class TestComplexRelationshipQueries:
    """Test complex queries involving multiple relationships."""

    def test_query_across_multiple_relationships(self, engine: Engine):
        """Test querying across multiple relationship levels."""
        with Session(engine) as session:
            author = Author(name="Complex Query Author", field="Astronomy")
            publisher = Publisher(name="Complex Query Pub", country="USA")
            book = Book(title="Complex Query Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-3333333333",
                pages=400,
                abstract="Complex query test",
            )
            book.detail.value = detail

            cat = Category(name="Astronomy", description="Space books")
            book.categories.append(cat)

            session.add(book)
            session.flush()

            # Query books with specific author field, category, and page count
            stmt = (
                select(Book)
                .join(models.Author)
                .join(models.BookDetail)
                .join(models.Book.categories)
                .where(
                    models.Author.field == "Astronomy",
                    models.Category.name == "Astronomy",
                    models.BookDetail.pages >= 300,
                )
            )

            books = session.execute(stmt).scalars().unique().all()

            assert len(books) >= 1
            assert any(b.title == "Complex Query Book" for b in books)

    def test_nested_relationship_loading(self, engine: Engine):
        """Test loading nested relationships."""
        with Session(engine) as session:
            author = Author(name="Nested Load Author", field="Literature")
            publisher = Publisher(name="Nested Load Pub", country="USA")
            book = Book(title="Nested Load Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            detail = BookDetail(
                isbn="978-4444444444",
                pages=250,
                abstract="Nested loading",
            )
            book.detail.value = detail

            session.add(book)
            session.flush()
            book_id = book.id

            # Clear session
            session.expunge_all()

            # Load book with nested relationships
            stmt = (
                select(Book)
                .where(Book["id"] == book_id)
                .options(
                    selectinload(models.Book.author),
                    selectinload(models.Book.detail),
                    selectinload(models.Book.publisher),
                )
            )

            loaded_book = session.execute(stmt).scalars().one()

            # All relationships should be loaded
            assert loaded_book.author.value.name == "Nested Load Author"
            assert loaded_book.detail.value.pages == 250
            assert loaded_book.publisher.value.name == "Nested Load Pub"
