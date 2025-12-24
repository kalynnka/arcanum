from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Protocol,
    Self,
    TypeVar,
    overload,
)

from pydantic.fields import FieldInfo

if TYPE_CHECKING:
    from arcanum.base import BaseTransmuter

T = TypeVar("T")


def like(column: Column[T], value: str) -> Expression[T]:
    return column.like(value)


def ilike(column: Column[T], value: str) -> Expression[T]:
    return column.ilike(value)


def not_like(column: Column[T], value: str) -> Expression[T]:
    return column.not_like(value)


def in_(column: Column[T], value: Iterable[T]) -> Expression[T]:
    return column.in_(value)


def not_in(column: Column[T], value: Iterable[T]) -> Expression[T]:
    return column.not_in(value)


def starts_with(column: Column[T], value: str) -> Expression[T]:
    return column.starts_with(value)


def ends_with(column: Column[T], value: str) -> Expression[T]:
    return column.ends_with(value)


@overload
def unwarp(value: Expression) -> ExpressionProtocol: ...
@overload
def unwarp(value: list[Expression | T]) -> tuple[ExpressionProtocol | T]: ...
@overload
def unwarp(value: set[Expression | T]) -> set[ExpressionProtocol | T]: ...
@overload
def unwarp(value: tuple[Expression | T, ...]) -> tuple[ExpressionProtocol | T, ...]: ...
@overload
def unwarp(value: T) -> T: ...
def unwarp(value: Expression | list | set | tuple | Any):
    if isinstance(value, Expression):
        return value.inner
    if isinstance(value, (list, set, tuple)):
        return tuple(unwarp(v) for v in value)
    return value


class ExpressionProtocol(Protocol):
    def __lt__(self, other: Any) -> Self: ...
    def __le__(self, other: Any) -> Self: ...
    def __eq__(self, other: Any) -> Self: ...
    def __ne__(self, other: Any) -> Self: ...
    def __gt__(self, other: Any) -> Self: ...
    def __ge__(self, other: Any) -> Self: ...
    def __contains__(self, other: Iterable) -> Self: ...

    def __and__(self, other: ExpressionProtocol) -> Self: ...
    def __or__(self, other: ExpressionProtocol) -> Self: ...
    def __invert__(self) -> Self: ...

    def like(self, other: str) -> Self: ...
    def ilike(self, other: str) -> Self: ...
    def not_like(self, other: str) -> Self: ...

    def in_(self, other: Iterable) -> Self: ...
    def not_in(self, other: Iterable) -> Self: ...

    def starts_with(self, other: str) -> Self: ...
    def ends_with(self, other: str) -> Self: ...

    def asc(self) -> Self: ...
    def desc(self) -> Self: ...


class Expression(Generic[T]):
    inner: ExpressionProtocol

    def __call__(self):
        return self.inner

    def __init__(self, inner):
        self.inner = inner

    def __lt__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__lt__(unwarp(other))
        return self

    def __le__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__le__(unwarp(other))
        return self

    def __eq__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__eq__(unwarp(other))
        return self

    def __ne__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__ne__(unwarp(other))
        return self

    def __gt__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__gt__(unwarp(other))
        return self

    def __ge__(self, other: T) -> Expression[T]:
        self.inner = self.inner.__ge__(unwarp(other))
        return self

    def __contains__(self, other: Iterable[T]) -> Expression[T]:
        self.inner = self.inner.__contains__(unwarp(other))
        return self

    def __and__(self, other: Expression) -> Expression[T]:
        self.inner = self.inner.__and__(unwarp(other))
        return self

    def __or__(self, other: Expression) -> Expression[T]:
        self.inner = self.inner.__or__(unwarp(other))
        return self

    def __invert__(self) -> Expression[T]:
        self.inner = self.inner.__invert__()
        return self

    def like(self, other: str) -> Expression[T]:
        self.inner = self.inner.like(other)
        return self

    def ilike(self, other: str) -> Expression[T]:
        self.inner = self.inner.ilike(other)
        return self

    def not_like(self, other: str) -> Expression[T]:
        self.inner = self.inner.not_like(other)
        return self

    def in_(self, other: Iterable[T]) -> Expression[T]:
        self.inner = self.inner.in_(unwarp(other))
        return self

    def not_in(self, other: Iterable[T]) -> Expression[T]:
        self.inner = self.inner.not_in(unwarp(other))
        return self

    def starts_with(self, other: str) -> Expression[T]:
        self.inner = self.inner.starts_with(other)
        return self

    def ends_with(self, other: str) -> Expression[T]:
        self.inner = self.inner.ends_with(other)
        return self


class Column(Expression[T]):
    __args__: tuple[T, ...]

    owner: type[BaseTransmuter]

    field_name: str
    used_name: str
    info: FieldInfo

    def __init__(self, owner: type[BaseTransmuter], field_name: str, info: FieldInfo):
        self.owner = owner
        self.field_name = field_name
        self.used_name = info.alias or field_name
        self.info = info

        super().__init__(inner=getattr(self.owner.__provider__, self.used_name))

    def asc(self) -> Self:
        self.inner = self.inner.asc()
        return self

    def desc(self) -> Self:
        self.inner = self.inner.desc()
        return self
