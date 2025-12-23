from __future__ import annotations

from typing import (
    Any,
    TypeVar,
    overload,
)

from pydantic import TypeAdapter
from sqlalchemy import (
    Column,
    Delete,
    Insert,
    Update,
    inspect,
)
from sqlalchemy.inspection import _InspectableTypeProtocol
from sqlalchemy.orm import InstrumentedAttribute
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
    adapter: TypeAdapter
    scalars_adapter: TypeAdapter

    def __init__(
        self,
        *entities: type[Any],
        adapter: TypeAdapter,
        scalar_adapter: TypeAdapter,
        **dialect_kw: Any,
    ) -> None:
        super().__init__(*entities, **dialect_kw)
        self.adapter = adapter
        self.scalar_adapter = scalar_adapter


def resolve_entities(
    entity: type[BaseProtocol | _InspectableTypeProtocol],
) -> tuple[type[Any], type[Any]]:
    if isinstance(entity, type) and issubclass(entity, BaseProtocol):
        return entity, entity.__provider__
    if isinstance(entity, (InstrumentedAttribute, Column)):
        inspected = inspect(entity, raiseerr=True)
        return inspected.expression.type.python_type, entity
    raise TypeError(
        f"Cannot resolve entity: {entity!r}, Currently only BaseProtocol, InstrumentedAttribute and Table.Column are supported."
    )


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
def select(  # type: ignore
    *entities: type[BaseProtocol | _InspectableTypeProtocol],
) -> AdaptedSelect[Any]:
    unwrapped_entities = []
    python_types = []
    for entity in entities:
        proto_type, provider_type = resolve_entities(entity)
        unwrapped_entities.append(provider_type)
        python_types.append(proto_type)
    adapter = TypeAdapter(tuple[*python_types])
    scalar_adapter = TypeAdapter(python_types[0])
    return AdaptedSelect(
        *unwrapped_entities,
        adapter=adapter,
        scalar_adapter=scalar_adapter,
    )
