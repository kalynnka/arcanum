from __future__ import annotations

from typing import Optional

from arcanum.association import Relation
from arcanum.base import BaseTransmuter
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel


class Foo(BaseTransmuter):
    __provider__: type[FooModel] = FooModel
    __provided__: FooModel

    id: int | None = None
    name: str
    bar: Relation[Optional[Bar]]


class Bar(BaseTransmuter):
    __provider__: type[BarModel] = BarModel
    __provided__: BarModel

    id: int | None = None
    data: str
    foo_id: int | None = None
    foo: Relation[Foo]
