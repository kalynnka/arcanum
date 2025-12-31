from __future__ import annotations

import uuid

import pytest

# Root-level conftest.py
# Shared test configuration and fixtures can be added here if needed.
# Materia-specific fixtures are now organized in their respective modules:
# - tests/noop/conftest.py for NoOp materia
# - tests/sqlalchemy/conftest.py for SQLAlchemy materia


@pytest.fixture(scope="function")
def test_id() -> uuid.UUID:
    """Generate a unique UUID for test case data isolation."""
    return uuid.uuid4()
