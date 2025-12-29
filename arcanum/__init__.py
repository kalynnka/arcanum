"""
arcanum - Glue to bind pydantic schemas with various datasources.

A Python library designed to seamlessly bind Pydantic schemas with various datasources,
eliminating the need to manually create templates, factories, and utilities repeatedly.
"""

__version__ = "0.1.0"
__author__ = "arcanum"
__email__ = "arcanum@example.com"

from arcanum.association import (
    Relation,
    RelationCollection,
    Relationship,
    Relationships,
)
from arcanum.base import BaseTransmuter
from arcanum.materia.base import NoOpMateria

__all__ = [
    "BaseTransmuter",
    "Relation",
    "RelationCollection",
    "Relationship",
    "Relationships",
    "NoOpMateria",
]
