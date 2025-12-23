from __future__ import annotations

from typing import (
    Any,
    TypeVar,
    overload,
)

from pydantic import TypeAdapter
from sqlalchemy import (
    Delete,
    Insert,
    Update,
)
from sqlalchemy.sql import Select

from arcanum.base import BaseProtocol

T = TypeVar("T")
T0 = TypeVar("T0")
T1 = TypeVar("T1")
S = TypeVar("S", bound=Insert | Select | Update | Delete)


class AdaptedSelect(Select[tuple[T]]):
    scalar_adapter: TypeAdapter[T]
    scalars_adapter: TypeAdapter[tuple[T]]

    def __init__(
        self,
        *entities: type[Any],
        scalar_adapter: TypeAdapter[T],
        scalars_adapter: TypeAdapter[tuple[T]],
        **dialect_kw: Any,
    ) -> None:
        super().__init__(*entities, **dialect_kw)
        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter


@overload
def select(entity: type[T]) -> AdaptedSelect[tuple[T]]: ...
@overload
def select(entity0: type[T0], entity1: type[T1]) -> AdaptedSelect[tuple[T0, T1]]: ...
def select(*entities: type[T]) -> AdaptedSelect[Any]:  # type: ignore
    unwrapped_entities = tuple(
        e.__provider__ if isinstance(e, type) and issubclass(e, BaseProtocol) else e
        for e in entities
    )
    if len(entities) == 1:
        scalar_adapter = TypeAdapter(entities[0])
        scalars_adapter = TypeAdapter(tuple[entities[0]])
    else:
        scalar_adapter = TypeAdapter(entities)
        scalars_adapter = TypeAdapter(tuple[entities])
    return AdaptedSelect(
        *unwrapped_entities,
        scalar_adapter=scalar_adapter,
        scalars_adapter=scalars_adapter,
    )
