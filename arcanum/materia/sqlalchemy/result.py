from __future__ import annotations

import operator
from collections.abc import Callable
from functools import cached_property
from typing import (
    Any,
    Iterator,
    Literal,
    Optional,
    Self,
    Sequence,
    TypeVar,
    overload,
)

from pydantic import TypeAdapter
from sqlalchemy import (
    FrozenResult,
    IteratorResult,
    Result,
    Row,
    ScalarResult,
)
from sqlalchemy.engine.result import (
    _NO_ROW,
    _R,
    FilterResult,
    ResultMetaData,
    _KeyIndexType,
    _UniqueFilterType,
    _WithKeys,
)
from sqlalchemy.engine.row import RowMapping
from sqlalchemy.sql.base import _generative

from arcanum.base import BaseTransmuter
from arcanum.materia.base import active_materia
from arcanum.utils import get_cached_adapter

_T = TypeVar("_T", bound=Any)
_TP = TypeVar("_TP", bound=tuple[Any, ...])


class AdaptedCommon(FilterResult[_R]):
    __slots__ = ()

    _real_result: Result[Any]
    _metadata: ResultMetaData

    def close(self) -> None:
        self._real_result.close()

    @property
    def closed(self) -> bool:
        return self._real_result.closed


class AdaptedResult(_WithKeys, AdaptedCommon[Row[_TP]]):
    entities: tuple[type[Any], ...]

    _real_result: Result[_TP]
    _row_logging_fn: Optional[Callable[[Row[Any]], Row[Any]]] = None

    def __init__(
        self,
        real_result: Result[_TP],
        entities: tuple[type[Any], ...] = (),
    ):
        self._real_result = real_result

        self._metadata = real_result._metadata
        self._unique_filter_state = real_result._unique_filter_state
        self._source_supports_scalars = real_result._source_supports_scalars
        self._post_creational_filter = None

        self.entities = entities

        # BaseCursorResult pre-generates the "_row_getter".  Use that
        # if available rather than building a second one
        if "_row_getter" in real_result.__dict__:
            self._set_memoized_attribute(
                "_row_getter", real_result.__dict__["_row_getter"]
            )

    @cached_property
    def adapter(self) -> TypeAdapter:
        """Get the adapter for tuple results."""
        return get_cached_adapter(tuple[*self.entities])

    @cached_property
    def scalar_adapter(self) -> TypeAdapter:
        """Get the adapter for scalar results."""
        return (
            get_cached_adapter(self.entities[0])
            if self.entities
            else TypeAdapter(object)
        )

    @cached_property
    def validate(self) -> bool:
        """Get validate flag from active materia context."""
        return active_materia.get().validate

    def _adapt(self, row: Any) -> Any:
        """Adapt a row of results based on the validate flag."""
        if self.validate:
            return self.adapter.validate_python(row)
        else:
            # Use model_construct for each entity in the row
            if isinstance(row, tuple):
                return tuple(
                    entity.model_construct(data=item)
                    if isinstance(entity, type) and issubclass(entity, BaseTransmuter)
                    else item
                    for entity, item in zip(self.entities, row)
                )
            return row

    def _adapt_scalar(self, item: Any) -> Any:
        """Adapt a scalar result based on the validate flag."""
        if self.validate:
            return self.scalar_adapter.validate_python(item)
        else:
            entity = self.entities[0] if self.entities else None
            if (
                entity
                and isinstance(entity, type)
                and issubclass(entity, BaseTransmuter)
            ):
                return entity.model_construct(data=item)
            return item

    @property
    def t(self) -> Self:
        return self

    @property
    def tuples(self) -> Self:
        return self

    @_generative
    def unique(self, strategy: Optional[_UniqueFilterType] = None) -> Self:
        """Apply unique filtering to the objects returned by this
        :class:`_asyncio.AsyncResult`.

        Refer to :meth:`_engine.Result.unique` in the synchronous
        SQLAlchemy API for a complete behavioral description.

        """
        self._unique_filter_state = (set(), strategy)
        return self

    def columns(self, *col_expressions: _KeyIndexType) -> Self:
        r"""Establish the columns that should be returned in each row.

        Refer to :meth:`_engine.Result.columns` in the synchronous
        SQLAlchemy API for a complete behavioral description.

        """
        return self._column_slices(col_expressions)

    def __iter__(self) -> Iterator[Row[_TP]]:
        for row in self._iter_impl():
            yield self._adapt(row)

    def __next__(self) -> Row[_TP]:
        return self._adapt(self._next_impl())

    def partitions(self, size: int | None = None) -> Iterator[tuple[Row[_TP]]]:
        while True:
            partition = self._manyrow_getter(self, size)
            if partition:
                yield tuple(self._adapt(row) for row in partition)
            else:
                break

    def fetchall(self) -> tuple[Row[_TP]]:
        return tuple(self._adapt(row) for row in self._allrows())

    def fetchone(self) -> Optional[Row[_TP]]:
        row = self._onerow_getter(self)
        if row is _NO_ROW:
            return None
        else:
            return self._adapt(row)

    def fetchmany(self, size: int | None = None) -> tuple[Row[_TP]]:
        return tuple(self._adapt(row) for row in self._manyrow_getter(size))

    def all(self) -> tuple[Row[_TP]]:
        return tuple(self._adapt(row) for row in self._allrows())

    def first(self) -> Optional[Row[_TP]]:
        return (
            self._adapt(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=False,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def one(self) -> Row[_TP]:
        return self._adapt(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=False,
            )
        )

    def one_or_none(self) -> Optional[Row[_TP]]:
        return (
            self._adapt(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    @overload
    def scalar(self: AdaptedResult[tuple[_T]]) -> Optional[_T]: ...

    @overload
    def scalar(self) -> Any: ...
    def scalar(self) -> Any:
        return self._adapt_scalar(
            self._only_one_row(
                raise_for_second_row=False,
                raise_for_none=False,
                scalar=True,
            )
        )

    @overload
    def scalar_one(self: AdaptedResult[tuple[_T]]) -> _T: ...

    @overload
    def scalar_one(self) -> Any: ...
    def scalar_one(self) -> Any:
        return self._adapt_scalar(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=True,
            )
        )

    @overload
    def scalar_one_or_none(self: AdaptedResult[tuple[_T]]) -> Optional[_T]: ...

    @overload
    def scalar_one_or_none(self) -> Optional[Any]: ...

    def scalar_one_or_none(self) -> Optional[Any]:
        return (
            self._adapt_scalar(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=True,
                )
            )
            else None
        )

    @overload
    def scalars(self: AdaptedResult[tuple[_T]]) -> AdaptedScalarResult[_T]: ...

    @overload
    def scalars(
        self: Result[tuple[_T]], index: Literal[0]
    ) -> AdaptedScalarResult[_T]: ...

    @overload
    def scalars(self, index: _KeyIndexType = 0) -> AdaptedScalarResult[Any]: ...
    def scalars(self, index: _KeyIndexType = 0) -> AdaptedScalarResult[Any]:
        return AdaptedScalarResult(
            self,
            index=index,
            entities=self.entities,
        )

    def freeze(self) -> AdaptedFrozenResult[_TP]:
        return AdaptedFrozenResult(
            self,
            entities=self.entities,
        )

    def mappings(self) -> AdaptedMappingResult:
        return AdaptedMappingResult(self)


class AdaptedScalarResult(ScalarResult[_R]):
    _entities: tuple[type[Any], ...]

    __slots__ = ()

    _generate_rows = False
    _real_result: AdaptedResult[Any]

    def __init__(
        self,
        real_result: AdaptedResult[Any],
        index: _KeyIndexType,
        entities: tuple[type[Any], ...] = (),
    ):
        self._unique_filter_state = real_result._unique_filter_state
        self._real_result = real_result

        if real_result._source_supports_scalars:
            self._metadata = real_result._metadata
            self._post_creational_filter = None
        else:
            self._metadata = real_result._metadata._reduce([index])
            self._post_creational_filter = operator.itemgetter(0)

        self._entities = entities

    @cached_property
    def scalar_adapter(self) -> TypeAdapter:
        """Get the adapter for scalar results."""
        return (
            get_cached_adapter(self._entities[0])
            if self._entities
            else TypeAdapter(object)
        )

    @cached_property
    def validate(self) -> bool:
        """Get validate flag from active materia context."""
        return active_materia.get().validate

    def _adapt_scalar(self, item: Any) -> Any:
        """Adapt a scalar result based on the validate flag."""
        if self.validate:
            return self.scalar_adapter.validate_python(item)
        else:
            entity = self._entities[0] if self._entities else None
            if (
                entity
                and isinstance(entity, type)
                and issubclass(entity, BaseTransmuter)
            ):
                return entity.model_construct(data=item)
            return item

    def unique(
        self,
        strategy: Optional[_UniqueFilterType] = None,
    ) -> Self:
        self._unique_filter_state = (set(), strategy)
        return self

    def __iter__(self) -> Iterator[_R]:
        for row in self._iter_impl():
            yield self._adapt_scalar(row)

    def __next__(self) -> _R:
        return self._adapt_scalar(self._next_impl())

    def partitions(self, size: int | None = None) -> Iterator[tuple[_R]]:
        while True:
            partition = self._manyrow_getter(self, size)
            if partition:
                yield tuple(self._adapt_scalar(scalar) for scalar in partition)
            else:
                break

    def fetchall(self) -> tuple[_R]:
        return tuple(self._adapt_scalar(scalar) for scalar in self._allrows())

    def fetchmany(self, size: int | None = None) -> tuple[_R]:
        return tuple(
            self._adapt_scalar(scalar) for scalar in self._manyrow_getter(size)
        )

    def all(self) -> tuple[_R]:
        return tuple(self._adapt_scalar(scalar) for scalar in self._allrows())

    def first(self) -> _R | None:
        return (
            self._adapt_scalar(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=False,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def one(self) -> _R:
        return self._adapt_scalar(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=False,
            )
        )

    def one_or_none(self) -> _R | None:
        return (
            self._adapt_scalar(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )


class AdaptedFrozenResult(FrozenResult[_TP]):
    data: Sequence[Any]
    _entities: tuple[type[Any], ...]

    def __init__(
        self,
        result: AdaptedResult[_TP],
        entities: tuple[type[Any], ...] = (),
    ):
        self.metadata = result._metadata._for_freeze()
        self._source_supports_scalars = result._source_supports_scalars
        self._attributes = result._attributes
        self._entities = entities

        if self._source_supports_scalars:
            self.data = self._adapt_scalar(result._real_result._raw_row_iterator())
        else:
            self.data = result.fetchall()

    @cached_property
    def adapter(self) -> TypeAdapter:
        """Get the adapter for tuple results."""
        return get_cached_adapter(tuple[*self._entities])

    @cached_property
    def scalar_adapter(self) -> TypeAdapter:
        """Get the adapter for scalar results."""
        return (
            get_cached_adapter(self._entities[0])
            if self._entities
            else TypeAdapter(object)
        )

    @cached_property
    def validate(self) -> bool:
        """Get validate flag from active materia context."""
        return active_materia.get().validate

    def _adapt_scalar(self, item: Any) -> Any:
        """Adapt a scalar result based on the validate flag."""
        if self.validate:
            return self.scalar_adapter.validate_python(item)
        else:
            entity = self._entities[0] if self._entities else None
            if (
                entity
                and isinstance(entity, type)
                and issubclass(entity, BaseTransmuter)
            ):
                return entity.model_construct(data=item)
            return item

    def rewrite_rows(self) -> Sequence[Sequence[Any]]:
        if self._source_supports_scalars:
            return [[elem] for elem in self.data]
        else:
            return [list(row) for row in self.data]

    def with_new_rows(self, tuple_data: Sequence[Row[_TP]]) -> AdaptedFrozenResult[_TP]:
        afr = AdaptedFrozenResult.__new__(AdaptedFrozenResult)
        afr.metadata = self.metadata
        afr._attributes = self._attributes
        afr._source_supports_scalars = self._source_supports_scalars
        afr._entities = self._entities

        if self._source_supports_scalars:
            afr.data = [d[0] for d in tuple_data]
        else:
            afr.data = tuple_data
        return afr

    def __call__(self) -> Result[_TP]:
        result = IteratorResult(
            self.metadata,
            iter(self.data),
        )
        result._attributes = self._attributes  # type: ignore
        result._source_supports_scalars = self._source_supports_scalars
        return result


class AdaptedMappingResult(_WithKeys, AdaptedCommon[RowMapping]):
    __slots__ = ()

    _generate_rows = True
    _post_creational_filter = operator.attrgetter("_mapping")

    _real_result: AdaptedResult[Any]
    _entities: tuple[type[Any], ...]

    def __init__(self, result: AdaptedResult[Any]):
        self._real_result = result
        self._metadata = result._metadata
        self._unique_filter_state = result._unique_filter_state
        if result._source_supports_scalars:
            self._metadata = self._metadata._reduce([0])

        self._entities = result.entities

    @cached_property
    def adapter(self) -> TypeAdapter:
        """Get the adapter for tuple results."""
        return get_cached_adapter(tuple[*self._entities])

    @cached_property
    def scalar_adapter(self) -> TypeAdapter:
        """Get the adapter for scalar results."""
        return (
            get_cached_adapter(self._entities[0])
            if self._entities
            else TypeAdapter(object)
        )

    @cached_property
    def validate(self) -> bool:
        """Get validate flag from active materia context."""
        return active_materia.get().validate

    def _adapt(self, row: Any) -> Any:
        """Adapt a row of results based on the validate flag."""
        if self.validate:
            return self.adapter.validate_python(row)
        else:
            # Use model_construct for each entity in the row
            if isinstance(row, (tuple, list)):
                return tuple(
                    entity.model_construct(data=item)
                    if isinstance(entity, type) and issubclass(entity, BaseTransmuter)
                    else item
                    for entity, item in zip(self._entities, row)
                )
            return row

    @_generative
    def unique(self, strategy: Optional[_UniqueFilterType] = None) -> Self:
        self._unique_filter_state = (set(), strategy)
        return self

    def columns(self, *col_expressions: _KeyIndexType) -> Self:
        return self._column_slices(col_expressions)

    def partitions(self, size: int | None = None) -> Iterator[Sequence[dict[str, Any]]]:
        while True:
            partition = self._manyrow_getter(self, size)
            if partition:
                yield tuple(
                    dict(zip(row.keys(), self._adapt(row.values())))
                    for row in partition
                )
            else:
                break

    def fetchone(self) -> Optional[dict[str, Any]]:
        row = self._onerow_getter(self)
        if row is _NO_ROW:
            return None
        else:
            return dict(zip(row.keys(), self._adapt(row.values())))

    def fetchmany(self, size: int | None = None) -> Sequence[dict[str, Any]]:
        return tuple(
            dict(zip(row.keys(), self._adapt(row.values())))
            for row in self._manyrow_getter(size)
        )

    def fetchall(self) -> Sequence[dict[str, Any]]:
        """A synonym for the :meth:`AdaptedMappingResult.all` method."""
        return tuple(
            dict(zip(row.keys(), self._adapt(row.values()))) for row in self._allrows()
        )

    def first(self) -> dict[str, Any] | None:
        return (
            dict(zip(row.keys(), self._adapt(row.values())))
            if (
                row := self._only_one_row(
                    raise_for_second_row=False,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def all(self) -> Sequence[dict[str, Any]]:
        return tuple(
            dict(zip(row.keys(), self._adapt(row.values()))) for row in self._allrows()
        )

    def one(self) -> dict[str, Any]:
        row = self._only_one_row(
            raise_for_second_row=True,
            raise_for_none=True,
            scalar=False,
        )
        return dict(zip(row.keys(), self._adapt(row.values())))

    def one_or_none(self) -> dict[str, Any] | None:
        return (
            dict(zip(row.keys(), self._adapt(row.values())))
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )
