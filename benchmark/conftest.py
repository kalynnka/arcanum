from __future__ import annotations

import random
from typing import Any

import pytest

from tests import schemas
from tests import transmuters as transmuters_module


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "baseline: mark test as baseline/control group (excluded from CodSpeed regression checks)",
    )


SEED = 42
random.seed(SEED)


@pytest.fixture(scope="session")
def simple_author_data() -> list[dict[str, Any]]:
    """Generate simple author data without relationships (once per session)."""
    random.seed(SEED)
    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    names = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
        "Octavia E. Butler",
        "Ray Bradbury",
        "Larry Niven",
        "Neal Stephenson",
        "Philip K. Dick",
        "Cixin Liu",
    ]
    return [
        {
            "id": i,
            "name": f"{random.choice(names)} {random.randint(100, 999)}",
            "field": random.choice(fields),
        }
        for i in range(100)
    ]


@pytest.fixture(scope="session")
def nested_book_data() -> list[dict[str, Any]]:
    """Generate book data with one level of nested relationships (once per session)."""
    random.seed(SEED)
    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    scifi_authors = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
        "Octavia E. Butler",
        "Ray Bradbury",
        "Larry Niven",
        "Neal Stephenson",
        "Philip K. Dick",
        "Cixin Liu",
    ]
    countries = ["USA", "UK", "Germany", "France", "Japan", "Canada"]
    adjectives = [
        "Galactic",
        "Neon",
        "Stellar",
        "Interstellar",
        "Synthetic",
        "Cybernetic",
    ]
    nouns = ["Odyssey", "Chronicles", "Protocol", "Singularity", "Nexus", "Expedition"]

    books = []
    for i in range(50):
        book = {
            "id": i,
            "title": f"The {random.choice(adjectives)} {random.choice(nouns)} {random.randint(1, 99)}",
            "year": random.randint(1990, 2024),
            "author": {
                "id": i,
                "name": f"{random.choice(scifi_authors)} {random.randint(100, 999)}",
                "field": random.choice(fields),
            },
            "publisher": {
                "id": i % 5,
                "name": f"Publisher {random.choice(['House', 'Press', 'Books'])} {random.randint(1, 50)}",
                "country": random.choice(countries),
            },
        }
        books.append(book)

    return books


@pytest.fixture(scope="session")
def deep_nested_data() -> list[dict[str, Any]]:
    """
    Generate author data with deeply nested book relationships (once per session).

    Creates author -> books -> author structure, but breaks circular reference
    by having book.author contain only scalar fields (no books array) to avoid
    infinite recursion during Pydantic validation.

    Only includes author, publisher to avoid complex nested requirements
    (e.g., BookDetail.book is required and would create recursion issues).
    """
    random.seed(SEED)
    author_count = 10
    books_per_author = 10
    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    scifi_authors = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
        "Octavia E. Butler",
        "Ray Bradbury",
        "Larry Niven",
        "Neal Stephenson",
        "Philip K. Dick",
        "Cixin Liu",
    ]
    countries = ["USA", "UK", "Germany", "France", "Japan"]
    adjectives = [
        "Galactic",
        "Neon",
        "Stellar",
        "Interstellar",
        "Synthetic",
        "Cybernetic",
    ]
    nouns = ["Odyssey", "Chronicles", "Protocol", "Singularity", "Nexus", "Expedition"]

    authors = []
    for i in range(author_count):
        num_books = books_per_author + random.randint(-2, 2)
        num_books = max(1, num_books)

        author_id = i
        author_name = f"{random.choice(scifi_authors)} {random.randint(100, 999)} {random.choice(['Sr.', 'Jr.', 'PhD', ''])}"
        author_field = random.choice(fields)

        books = []
        for j in range(num_books):
            book_dict = {
                "id": i * books_per_author + j,
                "title": f"The {random.choice(adjectives)} {random.choice(nouns)}",
                "year": random.randint(1995, 2024),
                "author": {
                    "id": author_id,
                    "name": author_name,
                    "field": author_field,
                },
                "publisher": {
                    "id": j % 5,
                    "name": f"Publisher {random.randint(1, 50)}",
                    "country": random.choice(countries),
                },
            }
            books.append(book_dict)

        author_dict = {
            "id": author_id,
            "name": author_name,
            "field": author_field,
            "books": books,
        }
        authors.append(author_dict)
    return authors


@pytest.fixture(scope="session")
def circular_data() -> list[dict[str, Any]]:
    """
    Generate company data with true circular references (Company -> Dept -> Employee -> back).

    Creates proper circular references between entities (for transmuter-only tests):
    - Company references its CEO (an Employee)
    - Departments reference their Company
    - Employees reference their Department and Company

    NOTE: Pydantic cannot handle these circular references due to infinite recursion.
    This fixture is intended for NoOpMateria transmuter tests only.
    """
    random.seed(SEED)
    company_count = 5
    industries = ["Tech", "Finance", "Healthcare", "Retail", "Manufacturing"]
    titles = ["Engineer", "Manager", "Analyst", "Director", "VP"]
    dept_names = ["Engineering", "Sales", "Marketing", "HR", "Finance", "R&D"]

    companies = []
    for c in range(company_count):
        num_depts = random.randint(2, 4)

        company_dict: dict[str, Any] = {
            "id": c,
            "name": f"Company {random.randint(100, 999)}",
            "industry": random.choice(industries),
        }

        departments = []
        all_employees = []

        for d in range(num_depts):
            num_employees = random.randint(3, 6)

            dept_dict: dict[str, Any] = {
                "id": c * 10 + d,
                "name": f"{random.choice(dept_names)} {random.randint(1, 99)}",
                "budget": random.randint(100000, 5000000),
                "company": company_dict,
            }

            employees = []
            for e in range(num_employees):
                employee_dict = {
                    "id": c * 100 + d * 10 + e,
                    "name": f"Employee {random.randint(100, 999)}",
                    "title": random.choice(titles),
                    "salary": random.randint(50000, 200000),
                    "department": dept_dict,
                    "company": company_dict,
                }
                employees.append(employee_dict)

            dept_dict["employees"] = employees
            all_employees.extend(employees)
            departments.append(dept_dict)

        company_dict["departments"] = departments
        company_dict["employees"] = all_employees
        company_dict["ceo"] = random.choice(all_employees) if all_employees else None

        companies.append(company_dict)
    return companies


@pytest.fixture(scope="session")
def simple_author_models(
    simple_author_data: list[dict[str, Any]],
) -> list[schemas.Author]:
    """Pre-validated Pydantic author models."""
    return [schemas.Author.model_validate(d) for d in simple_author_data]


@pytest.fixture(scope="session")
def simple_author_transmuters(
    simple_author_data: list[dict[str, Any]],
) -> list[transmuters_module.Author]:
    """Pre-validated NoOp author transmuters."""
    return [transmuters_module.Author.model_validate(d) for d in simple_author_data]


class MockAuthor:
    """Mock ORM-like author object."""

    def __init__(self, id: int, name: str, field: str):
        self.id = id
        self.name = name
        self.field = field


class MockPublisher:
    """Mock ORM-like publisher object."""

    def __init__(self, id: int, name: str, country: str):
        self.id = id
        self.name = name
        self.country = country


class MockBook:
    """Mock ORM-like book object with nested relationships."""

    def __init__(
        self,
        id: int,
        title: str,
        year: int,
        author: MockAuthor,
        publisher: MockPublisher,
    ):
        self.id = id
        self.title = title
        self.year = year
        self.author = author
        self.publisher = publisher


@pytest.fixture(scope="session")
def mock_author_objects() -> list[MockAuthor]:
    """Mock ORM-like author objects."""
    random.seed(SEED)
    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    names = [
        "Isaac Asimov",
        "Arthur C. Clarke",
        "Ursula K. Le Guin",
        "Robert A. Heinlein",
        "Frank Herbert",
        "William Gibson",
    ]
    return [
        MockAuthor(
            i,
            f"{random.choice(names)} {random.randint(1, 999)}",
            random.choice(fields),
        )
        for i in range(100)
    ]


@pytest.fixture(scope="session")
def mock_nested_book_objects() -> list[MockBook]:
    """Mock ORM-like book objects with nested relationships."""
    random.seed(SEED)
    fields = [
        "Astrophysics",
        "Robotics",
        "Cybernetics",
        "Xenobiology",
        "Quantum Physics",
        "Science Fiction",
    ]
    countries = ["USA", "UK", "Germany", "France", "Japan"]
    adjectives = ["Galactic", "Neon", "Stellar", "Interstellar", "Synthetic"]

    books = []
    for i in range(100):
        author = MockAuthor(
            i % 10, f"Author {random.randint(1, 999)}", random.choice(fields)
        )
        publisher = MockPublisher(
            i % 5, f"Publisher {random.randint(1, 50)}", random.choice(countries)
        )
        book = MockBook(
            i,
            f"The {random.choice(adjectives)} Book {random.randint(1, 99)}",
            random.randint(1990, 2024),
            author,
            publisher,
        )
        books.append(book)
    return books
