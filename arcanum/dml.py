from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Self, TypeVar, overload

from pydantic import TypeAdapter
from sqlalchemy import Delete, Insert, Update, UpdateBase
from sqlalchemy.sql._typing import (
    _ColumnExpressionArgument,
    _ColumnsClauseArgument,
    _DMLTableArgument,
)
from sqlalchemy.sql._typing import _TypedColumnClauseArgument as _TCCA
from sqlalchemy.sql.dml import DMLWhereBase, ValuesBase

from arcanum.base import BaseTransmuter
from arcanum.expression import Expression
from arcanum.selectable import AdaptedReturnRows, get_cached_adapter, resolve_entities

_TP = TypeVar("_TP", bound=tuple[Any, ...])

_T0 = TypeVar("_T0", bound=Any)
_T1 = TypeVar("_T1", bound=Any)
_T2 = TypeVar("_T2", bound=Any)
_T3 = TypeVar("_T3", bound=Any)
_T4 = TypeVar("_T4", bound=Any)
_T5 = TypeVar("_T5", bound=Any)
_T6 = TypeVar("_T6", bound=Any)
_T7 = TypeVar("_T7", bound=Any)


class AdaptedUpdateBase(ValuesBase):
    adapter: Optional[TypeAdapter] = None
    scalar_adapter: Optional[TypeAdapter] = None

    def returning(
        self,
        *cols: _ColumnsClauseArgument,
        sort_by_parameter_order: bool = False,
        **__kw: Any,
    ) -> UpdateBase:
        python_types, resolved_entities = zip(*(resolve_entities(col) for col in cols))  # pyright: ignore[reportArgumentType]
        self.adapter = get_cached_adapter(tuple[*python_types])
        self.scalar_adapter = get_cached_adapter(python_types[0])
        return super().returning(
            *resolved_entities, sort_by_parameter_order=sort_by_parameter_order, **__kw
        )


class AdaptedInsert(Insert, AdaptedUpdateBase):
    if TYPE_CHECKING:

        @overload
        def returning(
            self, __ent0: _TCCA[_T0], *, sort_by_parameter_order: bool = False
        ) -> AdaptedReturningInsert[tuple[_T0]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2, _T3]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2, _T3, _T4]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2, _T3, _T4, _T5]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            __ent7: _TCCA[_T7],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningInsert[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7]]: ...

        # END OVERLOADED FUNCTIONS self.returning

        @overload
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningInsert[Any]: ...

        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningInsert[Any]: ...


class AdaptedReturningInsert(AdaptedInsert, AdaptedReturnRows[_TP]): ...


class AdaptedDMLWhereBase(DMLWhereBase):
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


class AdaptedUpdate(Update, AdaptedDMLWhereBase, AdaptedUpdateBase):
    if TYPE_CHECKING:

        @overload
        def returning(
            self, __ent0: _TCCA[_T0], *, sort_by_parameter_order: bool = False
        ) -> AdaptedReturningUpdate[tuple[_T0]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2, _T3]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2, _T3, _T4]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2, _T3, _T4, _T5]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            __ent7: _TCCA[_T7],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningUpdate[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7]]: ...

        # END OVERLOADED FUNCTIONS self.returning

        @overload
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningUpdate[Any]: ...

        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningUpdate[Any]: ...


class AdaptedReturningUpdate(AdaptedUpdate, AdaptedReturnRows[_TP]): ...


class AdaptedDelete(Delete, AdaptedDMLWhereBase, AdaptedUpdateBase):
    if TYPE_CHECKING:

        @overload
        def returning(
            self, __ent0: _TCCA[_T0], *, sort_by_parameter_order: bool = False
        ) -> AdaptedReturningDelete[tuple[_T0]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2, _T3]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2, _T3, _T4]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2, _T3, _T4, _T5]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6]]: ...

        @overload
        def returning(
            self,
            __ent0: _TCCA[_T0],
            __ent1: _TCCA[_T1],
            __ent2: _TCCA[_T2],
            __ent3: _TCCA[_T3],
            __ent4: _TCCA[_T4],
            __ent5: _TCCA[_T5],
            __ent6: _TCCA[_T6],
            __ent7: _TCCA[_T7],
            *,
            sort_by_parameter_order: bool = False,
        ) -> AdaptedReturningDelete[tuple[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7]]: ...

        # END OVERLOADED FUNCTIONS self.returning

        @overload
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningDelete[Any]: ...

        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any,
        ) -> AdaptedReturningDelete[Any]: ...


class AdaptedReturningDelete(AdaptedDelete, AdaptedReturnRows[_TP]): ...


def resolve_transmuter(table: _DMLTableArgument) -> _DMLTableArgument:
    if isinstance(table, type) and issubclass(table, BaseTransmuter):
        return table.__provider__
    else:
        return table


def insert(table: _DMLTableArgument) -> AdaptedInsert:
    """Create an INSERT statement for the given table.

    Args:
        table: The table to insert into. Can be a BaseTransmuter protocol or a SQLAlchemy table.

    Returns:
        An AdaptedInsert instance that supports .returning() with proper type hints.
    """
    return AdaptedInsert(resolve_transmuter(table))


def update(table: _DMLTableArgument) -> AdaptedUpdate:
    """Create an UPDATE statement for the given table.

    Args:
        table: The table to update. Can be a BaseTransmuter protocol or a SQLAlchemy table.

    Returns:
        An AdaptedUpdate instance that supports .returning() with proper type hints.
    """
    return AdaptedUpdate(resolve_transmuter(table))


def delete(table: _DMLTableArgument) -> AdaptedDelete:
    """Create a DELETE statement for the given table.

    Args:
        table: The table to delete from. Can be a BaseTransmuter protocol or a SQLAlchemy table.

    Returns:
        An AdaptedDelete instance that supports .returning() with proper type hints.
    """
    return AdaptedDelete(resolve_transmuter(table))
