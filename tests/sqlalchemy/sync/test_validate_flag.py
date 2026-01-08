"""Test validate flag functionality for SqlalchemyMateria."""

from __future__ import annotations

from sqlalchemy import Engine

from arcanus.materia.base import active_materia
from arcanus.materia.sqlalchemy import SqlalchemyMateria


def test_validate_flag_default_true(engine: Engine):
    """Test that validate flag is True by default."""
    materia = SqlalchemyMateria()
    assert materia.validate is True


def test_validate_flag_can_be_set_false(engine: Engine):
    """Test that validate flag can be set to False."""
    materia = SqlalchemyMateria(validate=False)
    assert materia.validate is False


def test_with_validate_context_override(engine: Engine):
    """Test that with_validate can override the validate flag."""
    with SqlalchemyMateria(validate=True) as materia:
        # Original value
        assert materia.validate is True

        # Override to False
        with materia(validate=False) as new_materia:
            assert new_materia.validate is False
            assert materia.validate is True  # Original remains unchanged
            assert active_materia.get() is new_materia

        assert active_materia.get() is materia
