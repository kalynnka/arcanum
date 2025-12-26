from __future__ import annotations

from typing import (
    Any,
    Protocol,
    TypeVar,
    overload,
    runtime_checkable,
)

from pydantic import TypeAdapter
from sqlalchemy import (
    Column,
    Delete,
    Insert,
    Update,
)
from sqlalchemy.inspection import _InspectableTypeProtocol
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql._typing import (
    _ColumnExpressionArgument,
    _ColumnsClauseArgument,
    _TypedColumnClauseArgument,
)
from sqlalchemy.sql.selectable import TypedReturnsRows
from sqlalchemy.util.typing import Self

from arcanum.base import BaseTransmuter
from arcanum.expression import Column as ArcanumColumn
from arcanum.expression import Expression
from arcanum.utils import get_cached_adapter

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

_TCCA = _TypedColumnClauseArgument

S = TypeVar("S", bound=Insert | Select | Update | Delete)


@runtime_checkable
class AdaptedProtocol(Protocol):
    adapter: TypeAdapter
    scalar_adapter: TypeAdapter


class AdaptedReturnRows(AdaptedProtocol, TypedReturnsRows[_TP]):
    adapter: TypeAdapter
    scalar_adapter: TypeAdapter


class AdaptedSelect(Select, AdaptedReturnRows[_TP]):
    inherit_cache = True

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

    def where(
        self,
        *whereclause: _ColumnExpressionArgument[bool] | Expression[Any],
    ) -> Self:
        return super().where(
            *(
                clause() if isinstance(clause, Expression) else clause
                for clause in whereclause
            )  # pyright: ignore[reportArgumentType]
        )


def resolve_entities(
    entity: type[Any] | ArcanumColumn[Any] | _ColumnsClauseArgument[Any],
) -> tuple[type[Any], _ColumnsClauseArgument[Any]]:
    if isinstance(entity, type) and issubclass(entity, BaseTransmuter):
        return entity, entity.__provider__
    if isinstance(entity, ArcanumColumn):
        entity = entity()
    if isinstance(entity, Column):
        return entity.type.python_type, entity
    if isinstance(entity, InstrumentedAttribute):
        return entity.expression.type.python_type, entity
    raise TypeError(
        f"Cannot resolve entity: {entity!r}, Currently only BaseProtocol, InstrumentedAttribute and Table.Column are supported."
    )


@overload
def select(entity0: _TCCA[_T0] | ArcanumColumn[_T0]) -> AdaptedSelect[tuple[_T0]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0], entity1: _TCCA[_T1] | ArcanumColumn[_T1]
) -> AdaptedSelect[tuple[_T0, _T1]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
) -> AdaptedSelect[tuple[_T0, _T1, _T2]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
    entity5: _TCCA[_T5] | ArcanumColumn[_T5],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4, _T5]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
    entity5: _TCCA[_T5] | ArcanumColumn[_T5],
    entity6: _TCCA[_T6] | ArcanumColumn[_T6],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
    entity5: _TCCA[_T5] | ArcanumColumn[_T5],
    entity6: _TCCA[_T6] | ArcanumColumn[_T6],
    entity7: _TCCA[_T7] | ArcanumColumn[_T7],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
    entity5: _TCCA[_T5] | ArcanumColumn[_T5],
    entity6: _TCCA[_T6] | ArcanumColumn[_T6],
    entity7: _TCCA[_T7] | ArcanumColumn[_T7],
    entity8: _TCCA[_T8] | ArcanumColumn[_T8],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8]]: ...
@overload
def select(
    entity0: _TCCA[_T0] | ArcanumColumn[_T0],
    entity1: _TCCA[_T1] | ArcanumColumn[_T1],
    entity2: _TCCA[_T2] | ArcanumColumn[_T2],
    entity3: _TCCA[_T3] | ArcanumColumn[_T3],
    entity4: _TCCA[_T4] | ArcanumColumn[_T4],
    entity5: _TCCA[_T5] | ArcanumColumn[_T5],
    entity6: _TCCA[_T6] | ArcanumColumn[_T6],
    entity7: _TCCA[_T7] | ArcanumColumn[_T7],
    entity8: _TCCA[_T8] | ArcanumColumn[_T8],
    entity9: _TCCA[_T9] | ArcanumColumn[_T9],
) -> AdaptedSelect[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8, _T9]]: ...
def select(  # pyright: ignore[reportInconsistentOverload]
    *entities: type[BaseTransmuter | _InspectableTypeProtocol | Any]
    | ArcanumColumn[Any],
) -> AdaptedSelect[Any]:
    # TODO: maybe useful
    # https://docs.sqlalchemy.org/en/20/orm/queryguide/api.html#inspecting-entities-and-columns-from-orm-enabled-select-and-dml-statements
    python_types, unwrapped_entities = zip(
        *(resolve_entities(entity) for entity in entities)
    )

    return AdaptedSelect(
        *unwrapped_entities,
        adapter=get_cached_adapter(tuple[*python_types]),
        scalar_adapter=get_cached_adapter(python_types[0]),
    )
