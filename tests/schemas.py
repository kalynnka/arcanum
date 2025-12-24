from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic._internal._model_construction import NoInitField

from arcanum.association import Relation
from arcanum.base import BaseTransmuter
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel


class Foo(BaseTransmuter):
    __provider__: ClassVar[type[FooModel]] = FooModel
    __provided__: FooModel = NoInitField(init=False)

    id: int | None = None
    name: str
    bar: Relation[Optional[Bar]]


class Bar(BaseTransmuter):
    __provider__: ClassVar[type[BarModel]] = BarModel
    __provided__: BarModel = NoInitField(init=False)

    id: int | None = None
    data: str
    foo_id: int | None = None
    foo: Annotated[Foo, Relation[Foo]]
