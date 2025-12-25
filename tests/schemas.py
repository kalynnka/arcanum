from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic import Field
from pydantic._internal._model_construction import NoInitField

from arcanum.association import Relation, RelationCollection
from arcanum.base import BaseTransmuter, Identity
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel


class Foo(BaseTransmuter):
    __provider__: ClassVar[type[FooModel]] = FooModel
    __provided__: FooModel = NoInitField(init=False)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    bars: RelationCollection[Bar] = Field(default=RelationCollection())


class Bar(BaseTransmuter):
    __provider__: ClassVar[type[BarModel]] = BarModel
    __provided__: BarModel = NoInitField(init=False)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    data: str
    foo_id: int | None = None

    foo: Relation[Foo] = Field(default=Relation())
