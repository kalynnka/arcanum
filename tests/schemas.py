from __future__ import annotations

from typing import Annotated, Literal, Optional

from pydantic import ConfigDict, Field

from arcanum.association import Relation, RelationCollection
from arcanum.base import BaseTransmuter, Identity
from arcanum.materia.sqlalchemy import SqlalchemyMateria
from tests import models

sqlalchemy_materia = SqlalchemyMateria()


# Publisher schema (1-M with Book)
@sqlalchemy_materia.bless(models.Publisher)
class Publisher(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    country: str

    books: RelationCollection[Book] = Field(default_factory=RelationCollection)


# Author schema (1-M with Book)
@sqlalchemy_materia.bless(models.Author)
class Author(BaseTransmuter):
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

    books: RelationCollection[Book] = Field(default_factory=RelationCollection)


# BookDetail schema (1-1 with Book)
@sqlalchemy_materia.bless(models.BookDetail)
class BookDetail(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    isbn: str
    pages: int
    abstract: str
    book_id: int | None = None

    book: Relation[Book] = Field(default_factory=Relation)


# Category schema (M-M with Book)
@sqlalchemy_materia.bless(models.Category)
class Category(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    description: str

    books: RelationCollection[Book] = Field(default_factory=RelationCollection)


# Translator schema (optional 1-1 with Book)
@sqlalchemy_materia.bless(models.Translator)
class Translator(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    language: str

    book: Relation[Optional[Book]] = Field(default_factory=Relation)


# Review schema (optional M-1 with Book)
@sqlalchemy_materia.bless(models.Review)
class Review(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    reviewer_name: str
    rating: int = Field(ge=1, le=5)  # 1-5 stars
    comment: str
    book_id: int | None = None

    book: Relation[Optional[Book]] = Field(default_factory=Relation)


# Book schema (M-1 with Author, M-1 with Publisher, 1-1 with BookDetail, M-M with Category, optional 1-1 with Translator, optional 1-M with Reviews)
@sqlalchemy_materia.bless(models.Book)
class Book(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    title: str
    year: int
    author_id: int | None = None
    publisher_id: int | None = None
    translator_id: int | None = None

    author: Relation[Author] = Field(default_factory=Relation)
    publisher: Relation[Publisher] = Field(default_factory=Relation)
    translator: Relation[Optional[Translator]] = Field(
        default_factory=Relation
    )  # Optional 1-1
    detail: Relation[BookDetail] = Field(default_factory=Relation)
    categories: RelationCollection[Category] = Field(default_factory=RelationCollection)
    reviews: RelationCollection[Review] = Field(
        default_factory=RelationCollection
    )  # Optional 1-M
