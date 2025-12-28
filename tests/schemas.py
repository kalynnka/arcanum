from __future__ import annotations

from typing import Annotated, Optional

from pydantic import ConfigDict, Field

from arcanum.association import Relation, RelationCollection
from arcanum.base import BaseTransmuter, Identity
from arcanum.materia.sqlalchemy import SqlalchemyMateria
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel

sqlalchemy_materia = SqlalchemyMateria()


@sqlalchemy_materia.bless(FooModel)
class Foo(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    bars: RelationCollection[Bar] = Field(default=RelationCollection())


@sqlalchemy_materia.bless(BarModel)
class Bar(BaseTransmuter):
    model_config = ConfigDict(from_attributes=True)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    data: str
    foo_id: int | None = None

    foo: Relation[Foo] = Field(default=Relation())
