from __future__ import annotations

from typing import Annotated, Literal, Optional
from uuid import UUID

from pydantic import ConfigDict, Field

from arcanum.association import (
    Relation,
    RelationCollection,
    Relationship,
    Relationships,
)
from arcanum.base import BaseTransmuter, Identity
from arcanum.materia.sqlalchemy.base import SqlalchemyMateria
from tests import models

sqlalchemy_materia = SqlalchemyMateria()


class TestIdMixin:
    test_id: Optional[UUID] = Field(default=None, frozen=True)


# Publisher schema (1-M with Book)
@sqlalchemy_materia.bless(models.Publisher)
class Publisher(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    country: str

    books: RelationCollection[Book] = Relationships()


# Author schema (1-M with Book)
@sqlalchemy_materia.bless(models.Author)
class Author(BaseTransmuter, TestIdMixin):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    field: Literal[
        "Physics",
        "Biology",
        "Chemistry",
        "Literature",
        "History",
        "Quantum Physics",
        "Astronomy",
        "Dystopian Fiction",
    ]

    books: RelationCollection[Book] = Relationships()


# BookDetail schema (1-1 with Book)
@sqlalchemy_materia.bless(models.BookDetail)
class BookDetail(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    isbn: str
    pages: int
    abstract: str
    book_id: int | None = None

    book: Relation[Book] = Relationship()


# Category schema (M-M with Book)
@sqlalchemy_materia.bless(models.Category)
class Category(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    description: str | None = None

    books: RelationCollection[Book] = Relationships()


# Translator schema (optional 1-1 with Book)
@sqlalchemy_materia.bless(models.Translator)
class Translator(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    language: str

    book: Relation[Optional[Book]] = Relationship()


# Review schema (optional M-1 with Book)
@sqlalchemy_materia.bless(models.Review)
class Review(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    reviewer_name: str
    rating: int = Field(ge=1, le=5)  # 1-5 stars
    comment: str
    book_id: int | None = None

    book: Relation[Optional[Book]] = Relationship()


# Book schema (M-1 with Author, M-1 with Publisher, 1-1 with BookDetail, M-M with Category, optional 1-1 with Translator, optional 1-M with Reviews)
@sqlalchemy_materia.bless(models.Book)
class Book(TestIdMixin, BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    title: str
    year: int
    author_id: int | None = None
    publisher_id: int | None = None
    translator_id: int | None = None

    author: Relation[Author] = Relationship()
    publisher: Relation[Publisher] = Relationship()
    translator: Relation[Optional[Translator]] = Relationship()
    detail: Relation[BookDetail] = Relationship()
    categories: RelationCollection[Category] = Relationships()
    reviews: RelationCollection[Review] = Relationships()  # Optional 1-M
