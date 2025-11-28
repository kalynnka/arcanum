from __future__ import annotations

from arcanum.association import Relation
from arcanum.base import BaseProtocol
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel


class Foo(BaseProtocol):  # noqa: N801 - requested lowercase
    __provider__ = FooModel
    __provided__: FooModel

    id: int | None = None
    name: str
    bar: Relation[Bar]


class Bar(BaseProtocol):  # noqa: N801 - requested lowercase
    __provider__ = BarModel
    __provided__: BarModel

    id: int | None = None
    data: str
    foo_id: int | None = None
    foo: Relation[Foo]


__all__ = ["Foo", "Bar"]
