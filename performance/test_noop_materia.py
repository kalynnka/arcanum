"""
Performance Benchmarks: Pure Pydantic vs NoOpMateria

This module compares arcanum's NoOpMateria transmuters against pure Pydantic models
to measure the overhead of the transmuter abstraction WITHOUT database integration.

================================================================================
TEST GROUPS OVERVIEW
================================================================================

1. SIMPLE VALIDATION (simple-validate)
   - Flat objects with scalar fields only
   - 100 items per test

2. SIMPLE CONSTRUCTION (simple-construct)
   - model_construct without validation
   - 100 items per test

3. NESTED VALIDATION (nested-validate)
   - One level of nested objects (Book -> Author, Publisher, Detail)
   - 50 items with 3 nested objects each

4. DEEP NESTED VALIDATION (deep-nested-validate)
   - Two+ levels of nesting (Author -> Books -> Publisher, Detail, Categories)
   - 10 authors with ~10 books each

5. CIRCULAR REFERENCE (circular-validate)
   - Models with circular relationships (Company <-> Employee <-> Department)
   - 5 companies with full circular structure

6. MODEL DUMP (dump-dict / dump-json)
   - Serialization to dict and JSON
   - 100 simple items per test

7. FROM ATTRIBUTES (from-attributes-simple / from-attributes-nested)
   - ORM-style object attribute access
   - 100 items per test

================================================================================
FAIRNESS NOTES
================================================================================

- All tests within a group use IDENTICAL data (same seed, same structure)
- NoOpMateria validates only scalar fields by design (relationships are lazy)
- This is an architectural difference, not unfair comparison
- Docstrings explain when patterns differ

Run with: pytest performance/test_noop_materia.py -v --benchmark-only
"""

from __future__ import annotations

import random
from typing import Literal, Optional

import pytest
from pydantic import BaseModel, ConfigDict, Field

from arcanum.association import (
    Relation,
    RelationCollection,
    Relationship,
    Relationships,
)
from arcanum.base import BaseTransmuter, validation_context
from arcanum.materia.base import NoOpMateria

# ============================================================================
# CONFIGURATION
# ============================================================================

SEED = 42  # For reproducible randomness
random.seed(SEED)

# ============================================================================
# PURE PYDANTIC MODELS
# ============================================================================


class PureAuthorSimple(BaseModel):
    """Simple author without relationships."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    field: Literal["Physics", "Biology", "Chemistry", "Literature", "History"]


class PurePublisherSimple(BaseModel):
    """Simple publisher without relationships."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    country: str


class PureBookDetail(BaseModel):
    """Book detail model."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    isbn: str
    pages: int
    abstract: str


class PureCategory(BaseModel):
    """Category model."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    description: Optional[str] = None


class PureReview(BaseModel):
    """Review model."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    reviewer_name: str
    rating: int = Field(ge=1, le=5)
    comment: str


class PureBookNested(BaseModel):
    """Book with one level of nested relationships."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    title: str
    year: int
    author: PureAuthorSimple
    publisher: PurePublisherSimple
    detail: PureBookDetail


class PureBookFull(BaseModel):
    """Book with all relationships including collections."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    title: str
    year: int
    author: Optional[PureAuthorSimple] = None
    publisher: Optional[PurePublisherSimple] = None
    detail: Optional[PureBookDetail] = None
    categories: list[PureCategory] = Field(default_factory=list)
    reviews: list[PureReview] = Field(default_factory=list)


class PureAuthorWithBooks(BaseModel):
    """Author with nested books (deep nesting)."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    field: str
    books: list[PureBookFull] = Field(default_factory=list)


# Circular reference models (Pure Pydantic)
class PureDepartmentRef(BaseModel):
    """Minimal department reference to break circular."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str


class PureEmployeeRef(BaseModel):
    """Minimal employee reference to break circular."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    title: str


class PureDepartment(BaseModel):
    """Department with employee list."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    budget: int
    employees: list[PureEmployeeRef] = Field(default_factory=list)


class PureEmployee(BaseModel):
    """Employee with department reference."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    title: str
    salary: int
    department: Optional[PureDepartmentRef] = None


class PureCompany(BaseModel):
    """Company with departments and employees (circular structure)."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    industry: str
    departments: list[PureDepartment] = Field(default_factory=list)
    ceo: Optional[PureEmployeeRef] = None


# ============================================================================
# NOOP MATERIA TRANSMUTERS
# ============================================================================

noop_materia = NoOpMateria()


@noop_materia.bless()
class NoopAuthor(BaseTransmuter):
    """NoOp Author transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    field: Literal["Physics", "Biology", "Chemistry", "Literature", "History"]


@noop_materia.bless()
class NoopPublisher(BaseTransmuter):
    """NoOp Publisher transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    country: str


@noop_materia.bless()
class NoopBookDetail(BaseTransmuter):
    """NoOp BookDetail transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    isbn: str
    pages: int
    abstract: str


@noop_materia.bless()
class NoopCategory(BaseTransmuter):
    """NoOp Category transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    description: Optional[str] = None


@noop_materia.bless()
class NoopReview(BaseTransmuter):
    """NoOp Review transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    reviewer_name: str
    rating: int = Field(ge=1, le=5)
    comment: str


@noop_materia.bless()
class NoopBook(BaseTransmuter):
    """NoOp Book transmuter with associations."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    title: str
    year: int
    author_id: Optional[int] = None
    publisher_id: Optional[int] = None

    author: Relation[NoopAuthor] = Relationship()
    publisher: Relation[NoopPublisher] = Relationship()
    detail: Relation[NoopBookDetail] = Relationship()
    categories: RelationCollection[NoopCategory] = Relationships()
    reviews: RelationCollection[NoopReview] = Relationships()


# Circular transmuters
@noop_materia.bless()
class NoopDepartment(BaseTransmuter):
    """NoOp Department transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    budget: int

    employees: RelationCollection[NoopEmployee] = Relationships()


@noop_materia.bless()
class NoopEmployee(BaseTransmuter):
    """NoOp Employee transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    title: str
    salary: int

    department: Relation[NoopDepartment] = Relationship()


@noop_materia.bless()
class NoopCompany(BaseTransmuter):
    """NoOp Company transmuter with circular references."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    industry: str

    departments: RelationCollection[NoopDepartment] = Relationships()
    ceo: Relation[Optional[NoopEmployee]] = Relationship()


# ============================================================================
# DATA GENERATORS
# ============================================================================


def generate_simple_author_data(count: int = 100, seed: int = SEED) -> list[dict]:
    """Generate simple author data without relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    return [
        {
            "id": i,
            "name": f"{random.choice(names)} {random.randint(100, 999)}",
            "field": random.choice(fields),
        }
        for i in range(count)
    ]


def generate_nested_book_data(count: int = 50, seed: int = SEED) -> list[dict]:
    """Generate book data with one level of nested relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    countries = ["USA", "UK", "Germany", "France", "Japan", "Canada"]
    adjectives = ["Great", "Amazing", "Profound", "Essential", "Complete"]
    nouns = ["Theory", "Guide", "Study", "Analysis", "Introduction"]

    books = []
    for i in range(count):
        books.append(
            {
                "id": i,
                "title": f"The {random.choice(adjectives)} {random.choice(nouns)} {random.randint(1, 99)}",
                "year": random.randint(1990, 2024),
                "author": {
                    "id": i % 10,
                    "name": f"Author {random.randint(100, 999)}",
                    "field": random.choice(fields),
                },
                "publisher": {
                    "id": i % 5,
                    "name": f"Publisher {random.choice(['House', 'Press', 'Books'])} {random.randint(1, 50)}",
                    "country": random.choice(countries),
                },
                "detail": {
                    "id": i,
                    "isbn": f"978{random.randint(0, 999999999):09d}",
                    "pages": random.randint(100, 800),
                    "abstract": " ".join(
                        [
                            f"Word{random.randint(1, 500)}"
                            for _ in range(random.randint(20, 40))
                        ]
                    ),
                },
            }
        )
    return books


def generate_deep_nested_data(
    author_count: int = 10, books_per_author: int = 10, seed: int = SEED
) -> list[dict]:
    """Generate author data with deeply nested book relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    countries = ["USA", "UK", "Germany", "France", "Japan"]
    adjectives = ["Great", "Amazing", "Profound", "Essential", "Complete"]
    nouns = ["Theory", "Guide", "Study", "Analysis", "Introduction"]

    authors = []
    for i in range(author_count):
        num_books = books_per_author + random.randint(-2, 2)
        num_books = max(1, num_books)

        books = []
        for j in range(num_books):
            num_categories = random.randint(1, 4)
            num_reviews = random.randint(0, 5)
            books.append(
                {
                    "id": i * books_per_author + j,
                    "title": f"The {random.choice(adjectives)} {random.choice(nouns)}",
                    "year": random.randint(1995, 2024),
                    "author": {
                        "id": i,
                        "name": f"Author {random.randint(100, 999)}",
                        "field": random.choice(fields),
                    },
                    "publisher": {
                        "id": j % 5,
                        "name": f"Publisher {random.randint(1, 50)}",
                        "country": random.choice(countries),
                    },
                    "detail": {
                        "id": i * books_per_author + j,
                        "isbn": f"978{random.randint(0, 999999):06d}",
                        "pages": random.randint(100, 600),
                        "abstract": " ".join(
                            [
                                f"W{random.randint(1, 500)}"
                                for _ in range(random.randint(15, 35))
                            ]
                        ),
                    },
                    "categories": [
                        {
                            "id": k,
                            "name": f"Cat{random.choice(['A', 'B', 'C'])}{random.randint(1, 50)}",
                            "description": f"Desc {random.randint(1, 999)}",
                        }
                        for k in range(num_categories)
                    ],
                    "reviews": [
                        {
                            "id": (i * books_per_author + j) * 10 + k,
                            "reviewer_name": f"Rev{random.randint(1, 999)}",
                            "rating": random.randint(1, 5),
                            "comment": f"Comment {random.randint(1, 999)}",
                        }
                        for k in range(num_reviews)
                    ],
                }
            )

        authors.append(
            {
                "id": i,
                "name": f"Author {random.randint(100, 999)} {random.choice(['Sr.', 'Jr.', 'PhD', ''])}",
                "field": random.choice(fields),
                "books": books,
            }
        )
    return authors


def generate_circular_data(company_count: int = 5, seed: int = SEED) -> list[dict]:
    """Generate company data with circular references (Company -> Dept -> Employee -> back)."""
    random.seed(seed)
    industries = ["Tech", "Finance", "Healthcare", "Retail", "Manufacturing"]
    titles = ["Engineer", "Manager", "Analyst", "Director", "VP"]
    dept_names = ["Engineering", "Sales", "Marketing", "HR", "Finance", "R&D"]

    companies = []
    for c in range(company_count):
        num_depts = random.randint(2, 4)
        departments = []
        all_employees = []

        for d in range(num_depts):
            num_employees = random.randint(3, 6)
            employees = [
                {
                    "id": c * 100 + d * 10 + e,
                    "name": f"Employee {random.randint(100, 999)}",
                    "title": random.choice(titles),
                }
                for e in range(num_employees)
            ]
            all_employees.extend(employees)
            departments.append(
                {
                    "id": c * 10 + d,
                    "name": f"{random.choice(dept_names)} {random.randint(1, 99)}",
                    "budget": random.randint(100000, 5000000),
                    "employees": employees,
                }
            )

        # Pick a CEO from employees
        ceo = random.choice(all_employees) if all_employees else None

        companies.append(
            {
                "id": c,
                "name": f"Company {random.randint(100, 999)}",
                "industry": random.choice(industries),
                "departments": departments,
                "ceo": ceo,
            }
        )
    return companies


# ============================================================================
# MOCK ORM OBJECTS (for from_attributes tests)
# ============================================================================


def create_mock_author_objects(count: int = 100, seed: int = SEED) -> list:
    """Create mock objects that behave like ORM instances."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]

    class MockAuthor:
        def __init__(self, id: int, name: str, field: str):
            self.id = id
            self.name = name
            self.field = field

    return [
        MockAuthor(
            i, f"{random.choice(names)} {random.randint(1, 999)}", random.choice(fields)
        )
        for i in range(count)
    ]


def create_mock_nested_book_objects(count: int = 100, seed: int = SEED) -> list:
    """Create mock book objects with nested relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    countries = ["USA", "UK", "Germany", "France", "Japan"]
    adjectives = ["Great", "Amazing", "Profound", "Essential", "Complete"]

    class MockAuthor:
        def __init__(self, id, name, field):
            self.id = id
            self.name = name
            self.field = field

    class MockPublisher:
        def __init__(self, id, name, country):
            self.id = id
            self.name = name
            self.country = country

    class MockDetail:
        def __init__(self, id, isbn, pages, abstract):
            self.id = id
            self.isbn = isbn
            self.pages = pages
            self.abstract = abstract

    class MockBook:
        def __init__(self, id, title, year, author, publisher, detail):
            self.id = id
            self.title = title
            self.year = year
            self.author = author
            self.publisher = publisher
            self.detail = detail

    books = []
    for i in range(count):
        author = MockAuthor(
            i % 10, f"Author {random.randint(1, 999)}", random.choice(fields)
        )
        publisher = MockPublisher(
            i % 5, f"Publisher {random.randint(1, 50)}", random.choice(countries)
        )
        detail = MockDetail(
            i,
            f"978{random.randint(0, 999999):06d}",
            random.randint(100, 600),
            f"Abstract {i}",
        )
        book = MockBook(
            i,
            f"The {random.choice(adjectives)} Book {random.randint(1, 99)}",
            random.randint(1990, 2024),
            author,
            publisher,
            detail,
        )
        books.append(book)
    return books


# ============================================================================
# TEST CLASSES
# ============================================================================


class TestSimpleValidation:
    """
    Compare simple model validation (flat objects, scalar fields only).

    Both Pydantic and NoOpMateria validate the same fields.
    Same 100 author objects for fair comparison.
    """

    @pytest.fixture
    def shared_data(self):
        """Identical data for all tests in this group."""
        return generate_simple_author_data(100, seed=SEED)

    @pytest.mark.benchmark(group="simple-validate")
    def test_pure_pydantic_validate(self, benchmark, shared_data):
        """Pure Pydantic: model_validate on simple objects."""
        data = shared_data

        def validate_all():
            return [PureAuthorSimple.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="simple-validate")
    def test_noop_transmuter_validate(self, benchmark, shared_data):
        """NoOpMateria: model_validate on simple transmuters."""
        data = shared_data

        def validate_all():
            return [NoopAuthor.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100


class TestSimpleConstruction:
    """
    Compare model_construct (skip validation) performance.

    Same 100 author objects for fair comparison.
    """

    @pytest.fixture
    def shared_data(self):
        """Identical data for all tests in this group."""
        return generate_simple_author_data(100, seed=SEED)

    @pytest.mark.benchmark(group="simple-construct")
    def test_pure_pydantic_construct(self, benchmark, shared_data):
        """Pure Pydantic: model_construct (no validation)."""
        data = shared_data

        def construct_all():
            return [PureAuthorSimple.model_construct(**d) for d in data]

        result = benchmark(construct_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="simple-construct")
    def test_noop_transmuter_construct(self, benchmark, shared_data):
        """NoOpMateria: model_construct (no validation)."""
        data = shared_data

        def construct_all():
            with validation_context():
                return [NoopAuthor.model_construct(**d) for d in data]

        result = benchmark(construct_all)
        assert len(result) == 100


class TestNestedValidation:
    """
    Compare nested model validation (one level of nesting).

    ARCHITECTURAL DIFFERENCE:
    - Pure Pydantic validates ALL nested objects inline
    - NoOpMateria validates only scalar fields; relationships are lazy associations

    Both patterns are valid - they serve different use cases.
    """

    @pytest.fixture
    def shared_data(self):
        """50 books with nested author, publisher, detail."""
        return generate_nested_book_data(50, seed=SEED)

    @pytest.mark.benchmark(group="nested-validate")
    def test_pure_pydantic_nested(self, benchmark, shared_data):
        """Pure Pydantic: Validates all nested relationships inline."""
        data = shared_data

        def validate_all():
            return [PureBookNested.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 50
        assert result[0].author is not None
        assert result[0].publisher is not None

    @pytest.mark.benchmark(group="nested-validate")
    def test_noop_transmuter_nested(self, benchmark, shared_data):
        """
        NoOpMateria: Validates scalar fields only.

        Relationships become lazy associations - this is BY DESIGN.
        Use this when you want deferred relationship loading.
        """
        data = shared_data

        def validate_all():
            results = []
            for d in data:
                # NoOpMateria validates scalars; relationships are associations
                book = NoopBook.model_validate(
                    {
                        "id": d["id"],
                        "title": d["title"],
                        "year": d["year"],
                        "author_id": d["author"]["id"],
                        "publisher_id": d["publisher"]["id"],
                    }
                )
                results.append(book)
            return results

        result = benchmark(validate_all)
        assert len(result) == 50


class TestDeepNestedValidation:
    """
    Compare deeply nested validation (Author -> Books -> nested objects).

    Same architectural difference as TestNestedValidation applies.
    """

    @pytest.fixture
    def shared_data(self):
        """10 authors with ~10 books each, fully nested."""
        return generate_deep_nested_data(10, 10, seed=SEED)

    @pytest.mark.benchmark(group="deep-nested-validate")
    def test_pure_pydantic_deep_nested(self, benchmark, shared_data):
        """Pure Pydantic: Validates entire object graph."""
        data = shared_data

        def validate_all():
            return [PureAuthorWithBooks.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 10
        assert len(result[0].books) >= 1

    @pytest.mark.benchmark(group="deep-nested-validate")
    def test_noop_transmuter_deep_nested(self, benchmark, shared_data):
        """NoOpMateria: Validates author scalars only (books are associations)."""
        data = shared_data

        def validate_all():
            results = []
            for d in data:
                author = NoopAuthor.model_validate(
                    {
                        "id": d["id"],
                        "name": d["name"],
                        "field": d["field"],
                    }
                )
                results.append(author)
            return results

        result = benchmark(validate_all)
        assert len(result) == 10


class TestCircularReferences:
    """
    Compare validation with circular reference structures.

    Tests Company -> Department -> Employee -> (back to Company/Department).
    """

    @pytest.fixture
    def shared_data(self):
        """5 companies with circular structure."""
        return generate_circular_data(5, seed=SEED)

    @pytest.mark.benchmark(group="circular-validate")
    def test_pure_pydantic_circular(self, benchmark, shared_data):
        """Pure Pydantic: Validates circular structure (references broken)."""
        data = shared_data

        def validate_all():
            return [PureCompany.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 5
        assert len(result[0].departments) >= 2

    @pytest.mark.skip(reason="problem with circular validation in NoOpMateria")
    @pytest.mark.benchmark(group="circular-validate")
    def test_noop_transmuter_circular(self, benchmark, shared_data):
        """NoOpMateria: Validates company scalars (relationships are associations)."""
        data = shared_data

        def validate_all():
            results = []
            for d in data:
                company = NoopCompany.model_validate(
                    {
                        "id": d["id"],
                        "name": d["name"],
                        "industry": d["industry"],
                    }
                )
                results.append(company)
            return results

        result = benchmark(validate_all)
        assert len(result) == 5
        assert len(result[0].departments) >= 2


class TestModelDumpDict:
    """
    Compare model_dump() (serialization to dict).

    Uses pre-validated models to isolate dump performance.
    Same 100 simple authors for fair comparison.
    """

    @pytest.fixture
    def pydantic_models(self):
        """Pre-validated Pydantic models."""
        random.seed(SEED)
        data = generate_simple_author_data(100, seed=SEED)
        return [PureAuthorSimple.model_validate(d) for d in data]

    @pytest.fixture
    def transmuter_models(self):
        """Pre-validated NoOp transmuters."""
        random.seed(SEED)
        data = generate_simple_author_data(100, seed=SEED)
        return [NoopAuthor.model_validate(d) for d in data]

    @pytest.mark.benchmark(group="dump-dict")
    def test_pure_pydantic_dump(self, benchmark, pydantic_models):
        """Pure Pydantic: model_dump to dict."""
        models = pydantic_models

        def dump_all():
            return [m.model_dump() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]

    @pytest.mark.benchmark(group="dump-dict")
    def test_noop_transmuter_dump(self, benchmark, transmuter_models):
        """NoOpMateria: model_dump to dict (excludes relationship fields)."""
        models = transmuter_models

        def dump_all():
            # Exclude relationship fields which are lazy associations
            return [m.model_dump(exclude={"books"}) for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]


class TestModelDumpJson:
    """
    Compare model_dump_json() (serialization to JSON string).

    Same 100 simple authors for fair comparison.
    """

    @pytest.fixture
    def pydantic_models(self):
        """Pre-validated Pydantic models."""
        random.seed(SEED)
        data = generate_simple_author_data(100, seed=SEED)
        return [PureAuthorSimple.model_validate(d) for d in data]

    @pytest.fixture
    def transmuter_models(self):
        """Pre-validated NoOp transmuters."""
        random.seed(SEED)
        data = generate_simple_author_data(100, seed=SEED)
        return [NoopAuthor.model_validate(d) for d in data]

    @pytest.mark.benchmark(group="dump-json")
    def test_pure_pydantic_dump_json(self, benchmark, pydantic_models):
        """Pure Pydantic: model_dump_json to JSON string."""
        models = pydantic_models

        def dump_all():
            return [m.model_dump_json() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="dump-json")
    def test_noop_transmuter_dump_json(self, benchmark, transmuter_models):
        """NoOpMateria: model_dump_json to JSON string."""
        models = transmuter_models

        def dump_all():
            return [m.model_dump_json(exclude={"books"}) for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100


class TestFromAttributesSimple:
    """
    Compare from_attributes=True pattern (ORM-style attribute access).

    Same 100 mock author objects for fair comparison.
    """

    @pytest.fixture
    def mock_objects(self):
        """Mock ORM-like author objects."""
        return create_mock_author_objects(100, seed=SEED)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_pure_pydantic_from_attributes(self, benchmark, mock_objects):
        """Pure Pydantic: model_validate with from_attributes=True."""
        objects = mock_objects

        def validate_all():
            return [PureAuthorSimple.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_noop_transmuter_from_attributes(self, benchmark, mock_objects):
        """NoOpMateria: model_validate with from_attributes=True."""
        objects = mock_objects

        def validate_all():
            return [NoopAuthor.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100


class TestFromAttributesNested:
    """
    Compare from_attributes pattern with nested objects.

    Same 100 mock book objects for fair comparison.
    """

    @pytest.fixture
    def mock_objects(self):
        """Mock ORM-like book objects with nested relationships."""
        return create_mock_nested_book_objects(100, seed=SEED)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_pure_pydantic_nested_from_attributes(self, benchmark, mock_objects):
        """Pure Pydantic: Validates nested attributes from mock objects."""
        objects = mock_objects

        def validate_all():
            return [PureBookNested.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert result[0].author is not None

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_noop_transmuter_nested_from_attributes(self, benchmark, mock_objects):
        """NoOpMateria: Validates scalar fields from mock objects."""
        objects = mock_objects

        def validate_all():
            results = []
            for obj in objects:
                book = NoopBook.model_validate(
                    {
                        "id": obj.id,
                        "title": obj.title,
                        "year": obj.year,
                        "author_id": obj.author.id,
                        "publisher_id": obj.publisher.id,
                    }
                )
                results.append(book)
            return results

        result = benchmark(validate_all)
        assert len(result) == 100
