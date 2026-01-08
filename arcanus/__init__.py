"""
arcanus - Glue to bind pydantic schemas with various datasources.

A Python library designed to seamlessly bind Pydantic schemas with various datasources,
eliminating the need to manually create templates, factories, and utilities repeatedly.
"""

__version__ = "0.1.0"
__author__ = "arcanus"
__email__ = "arcanus@example.com"

from arcanus.association import (
    Relation,
    RelationCollection,
    Relationship,
    Relationships,
)
from arcanus.base import BaseTransmuter, validation_context
from arcanus.materia.base import NoOpMateria

__all__ = [
    "BaseTransmuter",
    "Relation",
    "RelationCollection",
    "Relationship",
    "Relationships",
    "NoOpMateria",
    "validation_context",
]
