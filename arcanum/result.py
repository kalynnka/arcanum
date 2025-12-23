from __future__ import annotations

import operator
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
    RowMapping,
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
from sqlalchemy.sql.base import _generative

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
        """proxies the .closed attribute of the underlying result object,
        if any, else raises ``AttributeError``.

        .. versionadded:: 2.0.0b3

        """
        return self._real_result.closed


class AdaptedResult(_WithKeys, AdaptedCommon[Row[_TP]]):
    scalar_adapter: TypeAdapter[_TP]
    scalars_adapter: TypeAdapter[tuple[_TP]]

    _real_result: Result[_TP]

    def __init__(
        self,
        real_result: Result[_TP],
        scalar_adapter: TypeAdapter[_TP],
        scalars_adapter: TypeAdapter[tuple[_TP]],
    ):
        self._real_result = real_result

        self._metadata = real_result._metadata
        self._unique_filter_state = real_result._unique_filter_state
        self._source_supports_scalars = real_result._source_supports_scalars
        self._post_creational_filter = None

        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter

        # BaseCursorResult pre-generates the "_row_getter".  Use that
        # if available rather than building a second one
        if "_row_getter" in real_result.__dict__:
            self._set_memoized_attribute(
                "_row_getter", real_result.__dict__["_row_getter"]
            )

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

    def __iter__(self) -> Iterator[tuple[_TP]]:
        for row in self._iter_impl():
            yield self.scalar_adapter.validate_python(row)

    def __next__(self) -> tuple[_TP]:
        return self.scalar_adapter.validate_python(self._next_impl())

    def partitions(self, size: int | None = None) -> Iterator[tuple[_TP]]:
        while True:
            partition = self._manyrow_getter(size)
            if partition:
                yield self.scalars_adapter.validate_python(partition)
            else:
                break

    def fetchall(self) -> tuple[_TP]:
        return self.scalars_adapter.validate_python(self._allrows())

    def fetchone(self) -> Optional[tuple[_TP]]:
        row = self._onerow_getter()
        if row is _NO_ROW:
            return None
        else:
            return self.scalar_adapter.validate_python(row)

    def fetchmany(self, size: int | None = None) -> tuple[_TP]:
        return self.scalars_adapter.validate_python(self._manyrow_getter(size))

    def all(self) -> tuple[_TP]:
        return self.scalars_adapter.validate_python(self._allrows())

    def first(self) -> Optional[tuple[_TP]]:
        return (
            self.scalar_adapter.validate_python(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=False,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def one(self) -> tuple[_TP]:
        return self.scalar_adapter.validate_python(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=False,
            )
        )

    def one_or_none(self) -> Optional[tuple[_TP]]:
        return (
            self.scalar_adapter.validate_python(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def scalar(self) -> Optional[tuple[_TP]]:
        return self.scalar_adapter.validate_python(
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
        return self.scalar_adapter.validate_python(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=False,
                scalar=False,
            )
        )

    @overload
    def scalar_one_or_none(self: AdaptedResult[tuple[_T]]) -> Optional[_T]: ...

    @overload
    def scalar_one_or_none(self) -> Optional[Any]: ...

    def scalar_one_or_none(self) -> Optional[Any]:
        return (
            self.scalar_adapter.validate_python(row)
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
            self,  # type: ignore
            index=index,
            scalar_adapter=self.scalar_adapter,
            scalars_adapter=self.scalars_adapter,
        )

    def freeze(self) -> AdaptedFrozenResult[_TP]:
        return AdaptedFrozenResult(
            self,
            scalar_adapter=self.scalar_adapter,
            scalars_adapter=self.scalars_adapter,
        )

    def mappings(self) -> AdaptedMappingResult:
        return AdaptedMappingResult(
            result=self._real_result,
            scalar_adapter=self.scalar_adapter,
            scalars_adapter=self.scalars_adapter,
        )


class AdaptedScalarResult(ScalarResult[_R]):
    scalar_adapter: TypeAdapter
    scalars_adapter: TypeAdapter

    __slots__ = ()

    _generate_rows = False

    def __init__(
        self,
        real_result: Result[Any],
        index: _KeyIndexType,
        scalar_adapter: TypeAdapter,
        scalars_adapter: TypeAdapter,
    ):
        self._unique_filter_state = real_result._unique_filter_state
        self._real_result = real_result

        if real_result._source_supports_scalars:
            self._metadata = real_result._metadata
            self._post_creational_filter = None
        else:
            self._metadata = real_result._metadata._reduce([index])
            self._post_creational_filter = operator.itemgetter(0)

        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter

    def unique(
        self,
        strategy: Optional[_UniqueFilterType] = None,
    ) -> Self:
        self._unique_filter_state = (set(), strategy)
        return self

    def __iter__(self) -> Iterator[_R]:
        for row in self._iter_impl():
            yield self.scalar_adapter.validate_python(row)

    def __next__(self) -> _R:
        return self.scalar_adapter.validate_python(self._next_impl())

    def partitions(self, size: int | None = None) -> Iterator[tuple[_R]]:
        while True:
            partition = self._manyrow_getter(size)
            if partition:
                yield self.scalars_adapter.validate_python(partition)
            else:
                break

    def fetchall(self) -> tuple[_R]:
        return self.scalars_adapter.validate_python(self._allrows())

    def fetchmany(self, size: int | None = None) -> tuple[_R]:
        return self.scalars_adapter.validate_python(self._manyrow_getter(size))

    def all(self) -> tuple[_R]:
        return self.scalars_adapter.validate_python(self._allrows())

    def first(self) -> _R | None:
        return (
            self.scalar_adapter.validate_python(row)
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
        return self.scalar_adapter.validate_python(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=False,
            )
        )

    def one_or_none(self) -> _R | None:
        return (
            self.scalar_adapter.validate_python(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )


class AdaptedMappingResult(_WithKeys, AdaptedCommon[RowMapping]):
    """A wrapper for a :class:`AdaptedResult` that returns dictionary
    values rather than :class:`_engine.Row` values.

    The :class:`AdaptedMappingResult` object is acquired by calling the
    :meth:`AdaptedResult.mappings` method.

    """

    scalar_adapter: TypeAdapter
    scalars_adapter: TypeAdapter

    __slots__ = ()

    _generate_rows = True

    _post_creational_filter = operator.attrgetter("_mapping")

    def __init__(
        self,
        result: Result[Any],
        scalar_adapter: TypeAdapter,
        scalars_adapter: TypeAdapter,
    ):
        self._real_result = result
        self._unique_filter_state = result._unique_filter_state
        self._metadata = result._metadata
        if result._source_supports_scalars:
            self._metadata = self._metadata._reduce([0])

        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter

    def unique(
        self,
        strategy: Optional[_UniqueFilterType] = None,
    ) -> Self:
        """Apply unique filtering to the objects returned by this
        :class:`AdaptedMappingResult`.

        """
        self._unique_filter_state = (set(), strategy)
        return self

    def columns(self, *col_expressions: _KeyIndexType) -> Self:
        r"""Establish the columns that should be returned in each row."""
        return self._column_slices(col_expressions)

    def partitions(self, size: Optional[int] = None) -> Iterator[RowMapping]:
        """Iterate through sub-lists of elements of the size given.

        Equivalent to :meth:`AdaptedResult.partitions` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        while True:
            partition = self._manyrow_getter(size)
            if partition:
                yield self.scalars_adapter.validate_python(partition)
            else:
                break

    def fetchall(self) -> tuple[RowMapping]:
        """A synonym for the :meth:`AdaptedMappingResult.all` method."""
        return self.scalars_adapter.validate_python(self._allrows())

    def fetchone(self) -> RowMapping | None:
        """Fetch one object.

        Equivalent to :meth:`AdaptedResult.fetchone` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        row = self._onerow_getter()
        if row is _NO_ROW:
            return None
        else:
            return self.scalar_adapter.validate_python(row)

    def fetchmany(self, size: Optional[int] = None) -> tuple[RowMapping]:
        """Fetch many rows.

        Equivalent to :meth:`AdaptedResult.fetchmany` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        return self.scalars_adapter.validate_python(self._manyrow_getter(size))

    def all(self) -> tuple[RowMapping]:
        """Return all rows in a list.

        Equivalent to :meth:`AdaptedResult.all` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        return self.scalars_adapter.validate_python(self._allrows())

    def __iter__(self) -> Iterator[RowMapping]:
        for row in self._iter_impl():
            yield self.scalar_adapter.validate_python(row)

    def __next__(self) -> RowMapping:
        row = self._next_impl()
        if row is _NO_ROW:
            raise StopIteration()
        else:
            return self.scalar_adapter.validate_python(row)

    def first(self) -> RowMapping | None:
        """Fetch the first object or ``None`` if no object is present.

        Equivalent to :meth:`AdaptedResult.first` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        return (
            self.scalar_adapter.validate_python(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=False,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def one_or_none(self) -> RowMapping | None:
        """Return at most one object or raise an exception.

        Equivalent to :meth:`AdaptedResult.one_or_none` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        return (
            self.scalar_adapter.validate_python(row)
            if (
                row := self._only_one_row(
                    raise_for_second_row=True,
                    raise_for_none=False,
                    scalar=False,
                )
            )
            else None
        )

    def one(self) -> RowMapping:
        """Return exactly one object or raise an exception.

        Equivalent to :meth:`AdaptedResult.one` except that
        :class:`_engine.RowMapping` values, rather than :class:`_engine.Row`
        objects, are returned.

        """
        return self.scalar_adapter.validate_python(
            self._only_one_row(
                raise_for_second_row=True,
                raise_for_none=True,
                scalar=False,
            )
        )


class AdaptedFrozenResult(FrozenResult[_TP]):
    data: Sequence[Any]

    scalar_adapter: TypeAdapter
    scalars_adapter: TypeAdapter

    def __init__(
        self,
        result: AdaptedResult[_TP],
        scalar_adapter: TypeAdapter,
        scalars_adapter: TypeAdapter,
    ):
        self.metadata = result._metadata._for_freeze()
        self._source_supports_scalars = result._source_supports_scalars
        self._attributes = result._attributes
        self.scalar_adapter = scalar_adapter
        self.scalars_adapter = scalars_adapter

        if self._source_supports_scalars:
            self.data = self.scalars_adapter.validate_python(
                result._real_result._raw_row_iterator()
            )
        else:
            self.data = result.fetchall()

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

        afr.scalar_adapter = self.scalar_adapter
        afr.scalars_adapter = self.scalars_adapter

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
