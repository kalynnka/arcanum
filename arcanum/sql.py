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
from sqlalchemy.sql._typing import _TypedColumnClauseArgument as _TCCA

from arcanum.base import BaseProtocol

_TP = TypeVar("_TP", bound=tuple[Any, ...])

_T0 = TypeVar("_T0", bound=Any)
_T1 = TypeVar("_T1", bound=Any)
_T2 = TypeVar("_T2", bound=Any)
_T3 = TypeVar("_T3", bound=Any)
_T4 = TypeVar("_T4", bound=Any)
_T5 = TypeVar("_T5", bound=Any)
_T6 = TypeVar("_T6", bound=Any)
_T7 = TypeVar("_T7", bound=Any)
_T8 = TypeVar("_T8", bound=Any)
_T9 = TypeVar("_T9", bound=Any)


S = TypeVar("S", bound=Insert | Select | Update | Delete)


class AdaptedSelect(Select[_TP]):
    scalar_adapter: TypeAdapter[_TP]
    scalars_adapter: TypeAdapter[tuple[_TP]]

    def __init__(
        self,
        *entities: type[Any],
        scalar_adapter: TypeAdapter[_TP],
        scalars_adapter: TypeAdapter[tuple[_TP]],
        **dialect_kw: Any,
    ) -> None:
        super().__init__(*entities, **dialect_kw)
        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter


@overload
def select(entity: _TCCA[_T0]) -> AdaptedSelect[tuple[_T0]]: ...
@overload
def select(
    entity0: _TCCA[_T0], entity1: _TCCA[_T1]
) -> AdaptedSelect[tuple[_T0, _T1]]: ...
@overload
def select(
    entity0: _TCCA[_T0], entity1: _TCCA[_T1], entity2: _TCCA[_T2]
) -> AdaptedSelect[tuple[_T0, _T1, _T2]]: ...
def select(*entities: type[Any]) -> AdaptedSelect[Any]:  # type: ignore
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
