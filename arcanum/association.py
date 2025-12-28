from __future__ import annotations

from abc import ABC
from copy import copy
from functools import cached_property, wraps
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Generator,
    Generic,
    Iterable,
    Literal,
    Optional,
    ParamSpec,
    Self,
    SupportsIndex,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    overload,
)

from pydantic import GetCoreSchemaHandler, TypeAdapter
from pydantic_core import core_schema
from sqlalchemy import inspect, select
from sqlalchemy.exc import InvalidRequestError, MissingGreenlet
from sqlalchemy.ext.associationproxy import AssociationProxyInstance
from sqlalchemy.orm import (
    InstrumentedAttribute,
    LoaderCallableStatus,
    WriteOnlyCollection,
    selectinload,
)
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.sql import functions
from sqlalchemy.util import greenlet_spawn

from arcanum.expression import Column, Expression
from arcanum.utils import get_cached_adapter

if TYPE_CHECKING:
    from _typeshed import SupportsRichComparison

    from arcanum.base import BaseTransmuter
    from arcanum.database import Session

A = TypeVar("A")
T = TypeVar("T", bound="BaseTransmuter")
Optional_T = TypeVar("Optional_T", bound="BaseTransmuter | Optional[BaseTransmuter]")

P = ParamSpec("P")
R = TypeVar("R")


def is_association(t: type) -> bool:
    origin = get_origin(t)
    arg = origin or t

    # Union of scalar types, e.g. Union[int, str] or Optional[str], which is Union[str, NoneType]
    if origin is Union or origin is UnionType:
        arg = get_args(t)[0]

    # Literal types, e.g. Literal["value1", "value2"]
    if origin is Literal:
        arg = type(get_args(t)[0])

    return issubclass(arg, Association)


def eager_load(*columns: Column, recursion_depth: int | None = None):
    return [
        selectinload(
            column.inner.local_attr
            if isinstance(column.inner, AssociationProxyInstance)
            else column.inner,  # pyright: ignore[reportArgumentType]
            recursion_depth=recursion_depth,
        )
        for column in columns
    ]


class Association(Generic[A], ABC):
    __args__: tuple[A, ...]
    __generic__: Type[A]
    __instance__: BaseTransmuter | None
    __loaded__: bool
    __payloads__: A | None

    field_name: str

    @classmethod
    def __pydantic_before_validator__(
        cls,
        value: Any,
        info: core_schema.ValidationInfo,
    ) -> Any:
        return value

    def __pydantic_after_validator__(self, info: core_schema.ValidationInfo) -> Self:
        """This method is called after the validation is done."""
        return self

    @classmethod
    def __get_pydantic_generic_schema__(
        cls,
        generic_type: Type[A],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        raise NotImplementedError()

    @classmethod
    def __get_pydantic_serialize_schema__(
        cls,
        generic_type: Type[A],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.SerSchema | None:
        raise NotImplementedError()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Type[Association[A]], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)

        if not args:
            raise TypeError(f"Generic type must be provided to the {source_type}.")

        generic_type = args[0]

        def validate(
            value: Any,
            handler: core_schema.ValidatorFunctionWrapHandler,
            info: core_schema.ValidationInfo,
        ) -> Association[A]:
            if not info.field_name:
                raise ValueError(
                    f"The association type {source_type} must be used as a model field."
                )

            value = cls.__pydantic_before_validator__(value, info)
            if isinstance(value, cls):
                instance = copy(value)
                instance.__payloads__ = handler(instance.__payloads__)
            else:
                instance = cls.__new__(cls)
                instance.__init__()
                instance.__payloads__ = handler(value)
            instance.__args__ = args
            instance.__generic__ = generic_type
            instance.field_name = info.field_name
            instance = instance.__pydantic_after_validator__(info)

            return instance

        schema = core_schema.chain_schema(
            [
                core_schema.with_info_wrap_validator_function(
                    validate,
                    cls.__get_pydantic_generic_schema__(generic_type, handler),
                    field_name=handler.field_name,
                ),
                core_schema.is_instance_schema(cls),
            ],
        )
        return core_schema.with_default_schema(
            core_schema.json_or_python_schema(
                json_schema=schema,
                python_schema=schema,
            ),
            default=cls(),
            validate_default=True,
            serialization=cls.__get_pydantic_serialize_schema__(generic_type, handler),
        )

    @cached_property
    def __validator__(self) -> TypeAdapter[A]:
        return get_cached_adapter(self.__generic__)

    @cached_property
    def used_name(self) -> str:
        return (
            alias
            if self.__instance__
            and (alias := type(self.__instance__).model_fields[self.field_name].alias)
            else self.field_name
        )

    def __init__(self, payloads: A | None = None):
        self.__instance__ = None
        self.__loaded__ = False
        self.__payloads__ = payloads

    def prepare(self, instance: BaseTransmuter, field_name: str):
        if self.__instance__ is not None:
            return
        self.__instance__ = instance
        self.field_name = field_name

    def validate_python(self, value: Any) -> A:
        """Validate the value against the type adapter."""
        return self.__validator__.validate_python(value)


class Relation(Association[Optional_T]):
    # new item and loaded item are shared the __payloads__ here
    __payloads__: Optional_T | None

    @classmethod
    def __get_pydantic_generic_schema__(
        cls, generic_type: type[Optional_T], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.union_schema(
            choices=[
                handler.generate_schema(generic_type),
                core_schema.none_schema(),
            ]
        )
        # TODO: strict the validation for lazy-load non-optional single relationship
        # return handler.generate_schema(generic_type)

    @classmethod
    def __get_pydantic_serialize_schema__(
        cls, generic_type: type[Optional_T], handler: GetCoreSchemaHandler
    ) -> core_schema.SerSchema | None:
        def serialize(value: Relation[Optional_T]) -> Any:
            return value.__payloads__

        return core_schema.plain_serializer_function_ser_schema(serialize)

    @classmethod
    def __pydantic_before_validator__(
        cls,
        value: Any,
        info: core_schema.ValidationInfo,
    ):
        # usually means relationship's loading is not yet completed
        return None if value is LoaderCallableStatus.NO_VALUE else value

    @property
    def __provided__(self) -> InstrumentedAttribute[Any]:
        if not self.__instance__:
            raise RuntimeError(
                f"The relation '{self.field_name}' is not yet prepared with an owner instance."
            )
        try:
            return getattr(self.__instance__.__provided__, self.used_name)
        except MissingGreenlet as missing_greenlet_error:
            self.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{self.field_name}' of {self.__instance__.__class__.__name__} for a greenlet is expected. Are you trying to get the relation in a sync context ? Await the {self.__instance__.__class__.__name__}.{self.field_name} instance to trigger the sqlalchemy async IO first."""
            ) from missing_greenlet_error
        except InvalidRequestError as invalid_request_error:
            self.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{self.field_name}' of {self.__instance__.__class__.__name__} for the relation's loading strategy is set to 'raise' in sqlalchemy. Specify the relationship with selectinload in statement options or change the loading strategy to 'select' or 'selectin' instead."""
            ) from invalid_request_error

    @__provided__.setter
    def __provided__(self, object: Any):
        if not self.__instance__:
            raise RuntimeError(
                f"The relation '{self.field_name}' is not yet prepared with an owner instance."
            )
        setattr(self.__instance__.__provided__, self.used_name, object)

    def prepare(self, instance: BaseTransmuter, field_name: str):
        super().prepare(instance, field_name)
        if not self.__loaded__ and self.__payloads__ is not None:
            self.__provided__ = self.__payloads__.__provided__

    @staticmethod
    def ensure_loaded(
        func: Callable[Concatenate[Relation[Optional_T], P], R],
    ) -> Callable[Concatenate[Relation[Optional_T], P], R]:
        @wraps(func)
        def wrapper(self: Relation[Optional_T], *args: P.args, **kwargs: P.kwargs) -> R:
            self._load()
            return func(self, *args, **kwargs)

        return wrapper

    def _load(self) -> Optional_T | None:
        # maybe during deepcopy from field default, or the relationship is already loaded
        if not self.__instance__ or self.__loaded__:
            return self.__payloads__

        if self.__payloads__ is not None and self.__payloads__.__provided__:
            self.__provided__ = self.__payloads__.__provided__
        else:
            self.__payloads__ = self.validate_python(self.__provided__)

        self.__loaded__ = True

        return self.__payloads__

    def __await__(self):
        return greenlet_spawn(self._load).__await__()

    @property
    @ensure_loaded
    def value(self) -> Optional_T | None:
        return self.__payloads__

    @value.setter
    @ensure_loaded
    def value(self, object: Optional_T):
        object = self.validate_python(object)
        if object is not None:
            self.__provided__ = object.__provided__
        else:
            self.__provided__ = None


class RelationCollection(list[T], Association[T]):
    # new items are held in __payloads__, loaded items are kept in the list itself
    __payloads__: Iterable[T] | None

    @classmethod
    def __get_pydantic_generic_schema__(
        cls,
        generic_type: Type[T],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.list_schema(handler.generate_schema(generic_type))

    @classmethod
    def __get_pydantic_serialize_schema__(
        cls, association: Type[T], handler: GetCoreSchemaHandler
    ) -> core_schema.SerSchema | None:
        def serialize(value: RelationCollection[T]) -> Any:
            return value

        return core_schema.plain_serializer_function_ser_schema(serialize)

    @classmethod
    def __pydantic_before_validator__(
        cls,
        value: Any,
        info: core_schema.ValidationInfo,
    ) -> Any:
        # usually means relationship's loading is not yet completed
        return [] if value is LoaderCallableStatus.NO_VALUE else value

    def __init__(self, payloads: Iterable[T] | None = None):
        super().__init__()
        self.__instance__ = None
        self.__loaded__ = False
        self.__payloads__ = payloads or []

    @cached_property
    def __provided__(self) -> InstrumentedList[Any]:
        # The return type is something like a list of T_Protocol's provider instances (orm objects),
        # which is actually returned by sqlalchemy's attr descriptor.

        if not self.__instance__:
            raise RuntimeError(
                f"The relation '{self.field_name}' is not yet prepared with an owner instance."
            )
        try:
            return getattr(self.__instance__.__provided__, self.used_name)
        except MissingGreenlet as missing_greenlet_error:
            self.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{self.field_name}' of {self.__instance__.__class__.__name__} for a greenlet is expected. Are you trying to get the relation in a sync context ? Await the {self.__instance__.__class__.__name__}.{self.field_name} instance to trigger the sqlalchemy async IO first."""
            ) from missing_greenlet_error
        except InvalidRequestError as invalid_request_error:
            self.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{self.field_name}' of {self.__instance__.__class__.__name__} for the relation's loading strategy is set to 'raise' in sqlalchemy. Specify the relationship with selectinload in statement options or change the loading strategy to 'select' or 'selectin' instead."""
            ) from invalid_request_error

    @cached_property
    def __list_validator__(self) -> TypeAdapter[list[T]]:
        return get_cached_adapter(list[self.__generic__])

    @overload
    def validate_python(self, value: T) -> T: ...
    @overload
    def validate_python(self, value: Iterable[Any]) -> list[T]: ...
    @overload
    def validate_python(self, value: Any) -> T: ...
    def validate_python(self, value: Any | Iterable[Any]) -> T | Iterable[T]:
        """Validate the value against the type adapter."""
        origin = get_origin(self.__generic__) or self.__generic__
        if isinstance(value, Iterable) and not isinstance(value, origin):
            return self.__list_validator__.validate_python(value)
        return self.__validator__.validate_python(value)

    def prepare(self, instance: BaseTransmuter, field_name: str):
        super().prepare(instance, field_name)
        if not self.__loaded__ and self.__payloads__:
            self.extend(self.__payloads__)
            self.__payloads__ = None

    @staticmethod
    def ensure_loaded(
        func: Callable[Concatenate[RelationCollection[T], P], R],
    ) -> Callable[Concatenate[RelationCollection[T], P], R]:
        @wraps(func)
        def wrapper(
            self: RelationCollection[T], *args: P.args, **kwargs: P.kwargs
        ) -> R:
            self._load()
            return func(self, *args, **kwargs)

        return wrapper

    def _load(self):
        # maybe during deepcopy from field default
        if not self.__instance__:
            return self

        # or the relationship is already loaded
        if self.__loaded__:
            return self

        if not len(self.__provided__) == super().__len__():
            # If the length of __provided__ is not equal to the length of self,
            # it means some items were not blessed into pydantic objects.
            super().clear()
            super().extend(self.validate_python(value=self.__provided__))

        self.__loaded__ = True

        return self

    def __await__(self):
        return greenlet_spawn(self._load).__await__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> T: ...
    @overload
    def __getitem__(self, index: slice) -> list[T]: ...
    @ensure_loaded
    def __getitem__(self, index: SupportsIndex | slice) -> T | list[T]:
        return super().__getitem__(index)

    @ensure_loaded
    def __iter__(self):
        return super().__iter__()

    @ensure_loaded
    def __len__(self):
        return super().__len__()

    @ensure_loaded
    def __contains__(self, key: T) -> bool:
        return super().__contains__(key)

    @ensure_loaded
    def __bool__(self):
        return bool(super())

    @overload
    def __setitem__(self, key: SupportsIndex, value: T) -> None: ...
    @overload
    def __setitem__(self, key: slice, value: Iterable[T]) -> None: ...
    @ensure_loaded
    def __setitem__(self, key: slice, value: T | Iterable[T]):
        if isinstance(value, Iterable):
            items = self.validate_python(value)
            self.__provided__[key] = [item.__provided__ for item in items]
            super().__setitem__(key, items)
        else:
            item = self.validate_python(value)
            self.__provided__[key] = item.__provided__
            super().__setitem__(key, item)

    @ensure_loaded
    def __delitem__(self, key: slice):
        del self.__provided__[key]
        super().__delitem__(key)

    @ensure_loaded
    def __add__(self, other: Iterable[T]):
        self.extend(other)
        return self

    @ensure_loaded
    def __iadd__(self, other: Iterable[T]):
        self.extend(other)
        return self

    def __mul__(self, other):
        raise NotImplementedError(
            "Left multiplication on relationship is not supported."
        )

    def __rmul__(self, other):
        raise NotImplementedError(
            "Right multiplication on relationship is not supported."
        )

    def __imul__(self, other):
        raise NotImplementedError(
            "Self multiplication on relationship is not supported."
        )

    @ensure_loaded
    def __eq__(self, other: list[T]):
        return super().__eq__(other)

    @ensure_loaded
    def __ne__(self, other: list[T]):
        return super().__ne__(other)

    @ensure_loaded
    def __lt__(self, other: list[T]):
        return super().__lt__(other)

    @ensure_loaded
    def __le__(self, other: list[T]):
        return super().__le__(other)

    @ensure_loaded
    def __gt__(self, other: list[T]):
        return super().__gt__(other)

    @ensure_loaded
    def __ge__(self, other: list[T]):
        return super().__ge__(other)

    @ensure_loaded
    def __repr__(self):
        return super().__repr__()

    @ensure_loaded
    def __str__(self):
        return super().__str__()

    @ensure_loaded
    def __reversed__(self):
        return super().__reversed__()

    @ensure_loaded
    def append(self, object: T):
        object = self.validate_python(object)
        self.__provided__.append(
            object.__provided__ if hasattr(object, "__provided__") else object
        )
        super().append(object)

    @ensure_loaded
    def extend(self, iterable: Iterable[T]):
        iterable = self.validate_python(iterable)
        self.__provided__.extend(
            (
                item.__provided__ if hasattr(item, "__provided__") else item
                for item in iterable
            )
        )
        super().extend(iterable)

    @ensure_loaded
    def clear(self):
        self.__provided__.clear()
        super().clear()

    @ensure_loaded
    def copy(self):
        return super().copy()

    @ensure_loaded
    def count(self, value: T) -> int:
        return super().count(value)

    @ensure_loaded
    def index(self, value, start=0, stop=None):
        if stop is None:
            return super().index(value, start)
        return super().index(value, start, stop)

    @ensure_loaded
    def insert(self, index: SupportsIndex, object: T):
        object = self.validate_python(object)
        self.__provided__.insert(index, object.__provided__)
        super().insert(index, object)

    @ensure_loaded
    def pop(self, index: SupportsIndex = -1):
        item = super().pop(index)
        self.__provided__.remove(item.__provided__)
        return item

    @ensure_loaded
    def remove(self, value: T):
        item: T = self.validate_python(value)
        self.__provided__.remove(item.__provided__)
        super().remove(value)

    @ensure_loaded
    def reverse(self):
        super().reverse()

    @ensure_loaded
    def sort(
        self,
        *,
        key: Callable[[T], SupportsRichComparison],
        reverse: bool = False,
    ):
        super().sort(key=key, reverse=reverse)


class PassiveRelationCollection(RelationCollection[T]):
    # new items kept in __payloads__, and passive relation collection never keep loaded items
    __payloads__: Iterable[T] | None
    batch_size: int = 10

    @classmethod
    def __pydantic_before_validator__(
        cls,
        value: Any,
        info: core_schema.ValidationInfo,
    ):
        # usually means relationship's loading is not yet completed
        return [] if value is LoaderCallableStatus.NO_VALUE else value

    @cached_property
    def __provided__(self) -> WriteOnlyCollection:
        if not self.__instance__:
            raise RuntimeError(
                f"The relation '{self.field_name}' is not yet prepared with an owner instance."
            )
        return getattr(self.__instance__.__provided__, self.used_name)

    @cached_property
    def __session__(self) -> Session:
        if not self.__instance__:
            raise RuntimeError(
                f"The relation '{self.field_name}' is not yet prepared with an owner instance."
            )
        return inspect(self.__instance__.__provided__).session

    @property
    def __loaded__(self) -> int:
        return super().__len__()

    def __iter__(self):
        for scalar in (
            self.__session__.execute(
                self.__provided__.select().execution_options(yield_per=1)
            )
            .scalars()
            .yield_per(self.batch_size)
        ):
            yield self.validate_python(scalar)

    def __len__(self) -> int:
        statement = self.__provided__.select().with_only_columns(functions.count())
        return self.__session__.execute(statement).scalar_one()

    def __contains__(self, key: T) -> bool:
        raise NotImplementedError(
            "Contain check on PassiveRelationCollection is not supported yet. Blocked by pk mapping in between."
        )

    def __bool__(self) -> bool:
        statement = self.__provided__.select().with_only_columns(functions.count())
        return self.__session__.execute(statement).scalar_one() > 0

    def prepare(self, instance: BaseTransmuter, field_name: str):
        super().prepare(instance, field_name)
        if self.__payloads__:
            # use append to add new assigned objects,
            # when an async dialect is choosed, extend would be expected called inside a greenlet,
            # while the prepare method is always called sync-ly inside host's getter which may lead to a greenlet await_only error.
            # WriteOnlyCollection.add uses session.add so it is always sync.
            for item in self.__payloads__:
                self.append(item)
        self.__payloads__ = None

    def list(
        self,
        limit: int | None = None,
        offset: int | None = None,
        # cursor: Any | None = None, # TODO: implement cursor pagination after pk mapping is ready
        order_bys: Iterable[Column] | None = None,
        expression: Expression | None = None,
        eager_loads: Iterable[Column] | None = None,
        **filters: Any,
    ) -> list[T]:
        statement = self.__provided__.select()
        if limit:
            statement = statement.limit(limit)
        if offset:
            statement = statement.offset(offset)
        if order_bys is not None:
            for order_by in order_bys:
                statement = statement.order_by(order_by())
        if filters:
            statement = statement.filter_by(**filters)
        if expression is not None:
            statement = statement.where(expression())
        if eager_loads is not None:
            statement = statement.options(*eager_load(*eager_loads))

        scalars = self.__session__.execute(statement).scalars().all()
        return self.validate_python(scalars)

    def partitions(
        self,
        limit: int | None = None,
        offset: int | None = None,
        # cursor: Any | None = None, # TODO: implement cursor pagination after pk mapping is ready
        order_bys: Iterable[Column] | None = None,
        size: int | None = None,
        expression: Expression | None = None,
        eager_loads: Iterable[Column] | None = None,
        **filters: Any,
    ) -> Generator[Iterable[T], None, None]:
        statement = self.__provided__.select().execution_options(
            yield_per=size or self.batch_size
        )
        if limit:
            statement = statement.limit(limit)
        if offset:
            statement = statement.offset(offset)
        if order_bys:
            for order_by in order_bys:
                statement = statement.order_by(order_by())
        if filters:
            statement = statement.filter_by(**filters)
        if expression:
            statement = statement.where(expression())
        if eager_loads:
            statement = statement.options(*eager_load(*eager_loads))

        for partition in (
            self.__session__.execute(statement)
            .scalars()
            .partitions(size=size or self.batch_size)
        ):
            yield self.validate_python(partition)

    def append(self, object: T):
        value = self.validate_python(object)
        self.__provided__.add(value.__provided__)

    def extend(self, iterable: Iterable[T]):
        iterable = self.validate_python(iterable)
        self.__session__.execute(
            self.__provided__.insert().values([item.__provided__ for item in iterable])
        )

    def clear(self):
        self.__session__.execute(self.__provided__.delete())

    def count(
        self,
        expression: Expression | None = None,
        **filters,
    ) -> int:
        statement = self.__provided__.select()
        if expression is not None:
            statement = statement.where(expression())
        if filters:
            statement = statement.filter_by(**filters)
        statement = select(functions.count()).select_from(statement.subquery())
        return self.__session__.execute(statement).scalar_one()

    def remove(self, value: T):
        item = self.validate_python(value)
        self.__provided__.remove(item.__provided__)
