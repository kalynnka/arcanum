from __future__ import annotations

from abc import ABC
from functools import cached_property, wraps
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Concatenate,
    Generic,
    Iterable,
    Literal,
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
from sqlalchemy.exc import InvalidRequestError, MissingGreenlet
from sqlalchemy.orm import InstrumentedAttribute, LoaderCallableStatus
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.util import greenlet_spawn

if TYPE_CHECKING:
    from arcanum.base import BaseTransmuter

T = TypeVar("T")
T_Protocol = TypeVar("T_Protocol", bound="BaseTransmuter")
OPT_Protocol = TypeVar("OPT_Protocol", bound="BaseTransmuter | None")

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
        arg = type(get_args(t)[0])  # type: ignore

    return issubclass(arg, Association)


class Association(Generic[T], ABC):
    __generic_adaptors__: ClassVar[dict[type, TypeAdapter[Any]]] = {}

    __args__: tuple[T, ...]
    __generic_protocol__: Type[T]
    __instance__: BaseTransmuter | None
    __loaded__: bool
    __payloads__: T | None

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
        generic_type: Type[T],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        raise NotImplementedError()

    @classmethod
    def __get_pydantic_serialize_schema__(
        cls,
        generic_type: Type[T],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.SerSchema | None:
        raise NotImplementedError()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Type[Association[T]], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)

        if not args:
            raise TypeError(f"Generic type must be provided to the {source_type}.")

        generic_type = args[0]

        def validate(
            value: Any,
            handler: core_schema.ValidatorFunctionWrapHandler,
            info: core_schema.ValidationInfo,
        ) -> Association[T]:
            if not info.field_name:
                raise ValueError(
                    f"The association type {source_type} must be used as a model field."
                )

            value = cls.__pydantic_before_validator__(value, info)
            if isinstance(value, cls):
                instance = value
                instance.__payloads__ = handler(instance.__payloads__)
            else:
                instance = cls.__new__(cls)
                instance.__init__()
                instance.__payloads__ = handler(value)
            instance.__args__ = args
            instance.__generic_protocol__ = generic_type
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
    def __validator__(self) -> TypeAdapter[T]:
        if self.__generic_protocol__ not in self.__generic_adaptors__:
            self.__generic_adaptors__[self.__generic_protocol__] = TypeAdapter(
                self.__generic_protocol__
            )
        return self.__generic_adaptors__[self.__generic_protocol__]

    @cached_property
    def used_name(self) -> str:
        return (
            alias
            if self.__instance__
            and (alias := type(self.__instance__).model_fields[self.field_name].alias)
            else self.field_name
        )

    def __init__(self, payloads: T | None = None):
        self.__instance__ = None
        self.__loaded__ = False
        self.__payloads__ = payloads

    def prepare(self, instance: BaseTransmuter, field_name: str):
        if self.__instance__ is not None:
            return
        self.__instance__ = instance
        self.field_name = field_name

    def validate_python(self, value: Any) -> T:
        """Validate the value against the type adapter."""
        return self.__validator__.validate_python(value)


class Relation(Association[OPT_Protocol]):
    @classmethod
    def __get_pydantic_generic_schema__(
        cls, generic_type: type[OPT_Protocol], handler: GetCoreSchemaHandler
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
        cls, generic_type: type[OPT_Protocol], handler: GetCoreSchemaHandler
    ) -> core_schema.SerSchema | None:
        def serialize(value: Relation[OPT_Protocol]) -> Any:
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
        func: Callable[Concatenate[Relation[OPT_Protocol], P], R],
    ) -> Callable[Concatenate[Relation[OPT_Protocol], P], R]:
        @wraps(func)
        def wrapper(
            self: Relation[OPT_Protocol], *args: P.args, **kwargs: P.kwargs
        ) -> R:
            if not self.__loaded__:
                self._load()
                self.__loaded__ = True

            return func(self, *args, **kwargs)

        return wrapper

    def _load(self):
        # maybe during deepcopy from field default
        if not self.__instance__:
            return
        if self.__payloads__ is not None and self.__payloads__.__provided__:
            self.__provided__ = self.__payloads__.__provided__
        else:
            self.__payloads__ = self.validate_python(self.__provided__)
        self.__loaded__ = True
        return

    def __await__(self):
        return greenlet_spawn(self._load).__await__()

    @property
    @ensure_loaded
    def value(self) -> OPT_Protocol:
        return self.__payloads__  # type: ignore

    @value.setter
    @ensure_loaded
    def value(self, object: OPT_Protocol):
        object = self.validate_python(object)
        if object is not None:
            self.__provided__ = object.__provided__
        else:
            self.__provided__ = None


class RelationCollection(list[T_Protocol], Association[T_Protocol]):
    __payloads__: Iterable[T_Protocol]

    @classmethod
    def __get_pydantic_generic_schema__(
        cls,
        generic_type: Type[T_Protocol],
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.list_schema(handler.generate_schema(generic_type))

    @classmethod
    def __get_pydantic_serialize_schema__(
        cls, association: Type[T_Protocol], handler: GetCoreSchemaHandler
    ) -> core_schema.SerSchema | None:
        def serialize(value: RelationCollection[T_Protocol]) -> Any:
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

    def __init__(self, payloads: Iterable[T_Protocol] | None = None):
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
    def __list_validator__(self) -> TypeAdapter[list[T_Protocol]]:
        if list[self.__generic_protocol__] not in self.__generic_adaptors__:
            self.__generic_adaptors__[list[self.__generic_protocol__]] = TypeAdapter(
                list[self.__generic_protocol__]
            )
        return self.__generic_adaptors__[list[self.__generic_protocol__]]

    @overload
    def validate_python(self, value: Iterable[Any]) -> Iterable[T_Protocol]: ...
    @overload
    def validate_python(self, value: Any) -> T_Protocol: ...
    def validate_python(
        self, value: Any | Iterable[Any]
    ) -> T_Protocol | Iterable[T_Protocol]:
        """Validate the value against the type adapter."""
        origin = get_origin(self.__generic_protocol__) or self.__generic_protocol__
        if isinstance(value, Iterable) and not isinstance(value, origin):
            return self.__list_validator__.validate_python(value)
        return self.__validator__.validate_python(value)

    def prepare(self, instance: BaseTransmuter, field_name: str):
        super().prepare(instance, field_name)
        if not self.__loaded__ and self.__payloads__:
            self.extend(self.__payloads__)

    @staticmethod
    def ensure_loaded(
        func: Callable[Concatenate[RelationCollection[T_Protocol], P], R],
    ) -> Callable[Concatenate[RelationCollection[T_Protocol], P], R]:
        @wraps(func)
        def wrapper(
            self: RelationCollection[T_Protocol], *args: P.args, **kwargs: P.kwargs
        ) -> R:
            if not self.__loaded__:
                self._load()
                self.__loaded__ = True

            return func(self, *args, **kwargs)

        return wrapper

    def _load(self):
        # maybe during deepcopy from field default
        if not self.__instance__:
            return
        existing_provided = set(self.__provided__)
        self.__loaded__ = True

        if new_items := [
            item.__provided__
            for item in self
            if hasattr(item, "__provided__")
            and item.__provided__ not in existing_provided
        ]:
            self.__provided__.extend(new_items)

        if len(self.__provided__) != len(self):
            # If the length of __provided__ is not equal to the length of self,
            # it means some items were not blessed into pydantic objects.
            super().clear()
            super().extend(self.validate_python(value=self.__provided__))
        return

    def __await__(self):
        coro = greenlet_spawn(self._load).__await__()
        return coro

    @overload
    def __getitem__(self, index: SupportsIndex) -> T_Protocol: ...
    @overload
    def __getitem__(self, index: slice) -> list[T_Protocol]: ...
    @ensure_loaded
    def __getitem__(
        self, index: SupportsIndex | slice
    ) -> T_Protocol | list[T_Protocol]:
        return super().__getitem__(index)

    @ensure_loaded
    def __iter__(self):
        return super().__iter__()

    @ensure_loaded
    def __len__(self):
        return super().__len__()

    @ensure_loaded
    def __contains__(self, key: T_Protocol) -> bool:
        return super().__contains__(key)

    @ensure_loaded
    def __bool__(self):
        return bool(super())

    @overload
    def __setitem__(self, key: SupportsIndex, value: T_Protocol) -> None: ...
    @overload
    def __setitem__(self, key: slice, value: Iterable[T_Protocol]) -> None: ...
    @ensure_loaded
    def __setitem__(
        self, key: SupportsIndex | slice, value: T_Protocol | Iterable[T_Protocol]
    ):
        if isinstance(value, Iterable):
            items = self.validate_python(value)
            self.__provided__[key] = [item.__provided__ for item in items]
            super().__setitem__(key, items)  # type: ignore
        else:
            item = self.validate_python(value)
            self.__provided__[key] = item.__provided__
            super().__setitem__(key, item)

    @ensure_loaded
    def __delitem__(self, key: SupportsIndex | slice):
        del self.__provided__[key]
        super().__delitem__(key)

    @ensure_loaded
    def __add__(self, other: Iterable[T_Protocol]):
        self.extend(other)
        return self

    @ensure_loaded
    def __iadd__(self, other: Iterable[T_Protocol]):
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
    def __eq__(self, other: list[T_Protocol]):
        return super().__eq__(other)

    @ensure_loaded
    def __ne__(self, other: list[T_Protocol]):
        return super().__ne__(other)

    @ensure_loaded
    def __lt__(self, other: list[T_Protocol]):
        return super().__lt__(other)

    @ensure_loaded
    def __le__(self, other: list[T_Protocol]):
        return super().__le__(other)

    @ensure_loaded
    def __gt__(self, other: list[T_Protocol]):
        return super().__gt__(other)

    @ensure_loaded
    def __ge__(self, other: list[T_Protocol]):
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
    def append(self, object: T_Protocol):
        object = self.validate_python(object)  # type: ignore
        self.__provided__.append(
            object.__provided__ if hasattr(object, "__provided__") else object
        )
        super().append(object)

    @ensure_loaded
    def extend(self, iterable: Iterable[T_Protocol]):
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
    def count(self, value: T_Protocol) -> int:
        return super().count(value)

    @ensure_loaded
    def index(self, value, start=0, stop=None):
        if stop is None:
            return super().index(value, start)
        return super().index(value, start, stop)

    @ensure_loaded
    def insert(self, index: SupportsIndex, object: T_Protocol):
        object = self.validate_python(object)  # type: ignore
        self.__provided__.insert(index, object.__provided__)
        super().insert(index, object)

    @ensure_loaded
    def pop(self, index: SupportsIndex = -1):
        item = super().pop(index)
        self.__provided__.remove(item.__provided__)
        return item

    @ensure_loaded
    def remove(self, value: T_Protocol):
        item: T_Protocol = self.validate_python(value)  # type: ignore
        self.__provided__.remove(item.__provided__)
        super().remove(value)

    @ensure_loaded
    def reverse(self):
        super().reverse()

    @ensure_loaded
    def sort(
        self, *, key: Callable[[T_Protocol], Any] | None = None, reverse: bool = False
    ):
        super().sort(key=key, reverse=reverse)  # type: ignore
