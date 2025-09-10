from __future__ import annotations

from protocollum.core import BaseProtocol
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel


class Foo(BaseProtocol):  # noqa: N801 - requested lowercase
    id: int | None = None
    name: str

    __provider__ = FooModel


class Bar(BaseProtocol):  # noqa: N801 - requested lowercase
    id: int | None = None
    data: str
    foo_id: int | None = None

    __provider__ = BarModel


__all__ = ["Foo", "Bar"]
