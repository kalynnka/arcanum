from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from arcanum.base import TransmuterProxiedMixin


class Base(DeclarativeBase, TransmuterProxiedMixin): ...


# Secondary table for M-M relationship between Book and Category
class BookCategory(Base):
    __tablename__ = "book_category"

    book_id: Mapped[int] = mapped_column(
        ForeignKey("book.id", ondelete="CASCADE"),
        primary_key=True,
    )
    category_id: Mapped[int] = mapped_column(
        ForeignKey("category.id", ondelete="CASCADE"),
        primary_key=True,
    )


# Publisher (1-M with Book)
class Publisher(Base):
    __tablename__ = "publisher"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    country: Mapped[str] = mapped_column(String(50), nullable=False)

    books: Mapped[list[Book]] = relationship(
        back_populates="publisher",
        uselist=True,
        cascade="save-update, merge, delete, delete-orphan",
    )


# Author (1-M with Book)
class Author(Base):
    __tablename__ = "author"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    field: Mapped[str] = mapped_column(
        String(100), nullable=False
    )  # e.g., "Physics", "Biology"

    books: Mapped[list[Book]] = relationship(
        back_populates="author",
        uselist=True,
        cascade="save-update, merge, delete, delete-orphan",
    )


# Translator (optional 1-1 with Book)
class Translator(Base):
    __tablename__ = "translator"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    language: Mapped[str] = mapped_column(String(50), nullable=False)

    # Optional 1-1: A translator may have translated one book (or none in this simplified model)
    book: Mapped[Book | None] = relationship(
        back_populates="translator",
        uselist=False,
    )


# Book (M-1 with Author, M-1 with Publisher, 1-1 with BookDetail, M-M with Category, optional 1-1 with Translator)
class Book(Base):
    __tablename__ = "book"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)

    author_id: Mapped[int] = mapped_column(
        ForeignKey("author.id", ondelete="CASCADE"),
        nullable=False,
    )
    author: Mapped[Author] = relationship(back_populates="books", uselist=False)

    publisher_id: Mapped[int] = mapped_column(
        ForeignKey("publisher.id", ondelete="CASCADE"),
        nullable=False,
    )
    publisher: Mapped[Publisher] = relationship(back_populates="books", uselist=False)

    # Optional 1-1 relationship with Translator (for translated books)
    translator_id: Mapped[int | None] = mapped_column(
        ForeignKey("translator.id", ondelete="SET NULL"),
        nullable=True,
    )
    translator: Mapped[Translator | None] = relationship(
        back_populates="book",
        uselist=False,
    )

    # 1-1 relationship with BookDetail
    detail: Mapped[BookDetail | None] = relationship(
        back_populates="book",
        uselist=False,
        cascade="save-update, merge, delete, delete-orphan",
    )

    # M-M relationship with Category via secondary table
    categories: Mapped[list[Category]] = relationship(
        secondary=BookCategory.__table__,
        back_populates="books",
        uselist=True,
    )

    # Optional 1-M relationship with Review
    reviews: Mapped[list[Review]] = relationship(
        back_populates="book",
        uselist=True,
        cascade="save-update, merge, delete, delete-orphan",
    )


# BookDetail (1-1 with Book)
class BookDetail(Base):
    __tablename__ = "book_detail"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isbn: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    pages: Mapped[int] = mapped_column(Integer, nullable=False)
    abstract: Mapped[str] = mapped_column(Text, nullable=False)

    book_id: Mapped[int] = mapped_column(
        ForeignKey("book.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    book: Mapped[Book] = relationship(back_populates="detail", uselist=False)


# Category (M-M with Book)
class Category(Base):
    __tablename__ = "category"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String(200), nullable=False)

    books: Mapped[list[Book]] = relationship(
        secondary=BookCategory.__table__,
        back_populates="categories",
        uselist=True,
    )


# Review (optional 1-M with Book)
class Review(Base):
    __tablename__ = "review"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reviewer_name: Mapped[str] = mapped_column(String(100), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)  # 1-5 stars
    comment: Mapped[str] = mapped_column(Text, nullable=False)

    # Optional M-1: A review may optionally be associated with a book
    book_id: Mapped[int | None] = mapped_column(
        ForeignKey("book.id", ondelete="CASCADE"),
        nullable=True,
    )
    book: Mapped[Book | None] = relationship(back_populates="reviews", uselist=False)


__all__ = [
    "Base",
    "Publisher",
    "Author",
    "Book",
    "BookDetail",
    "Category",
    "BookCategory",
    "Translator",
    "Review",
]
