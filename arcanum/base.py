from __future__ import annotations

import contextlib
from abc import ABC
from contextvars import ContextVar
from typing import (
    Any,
    ClassVar,
    Self,
    TypeVar,
    dataclass_transform,
    get_origin,
    get_type_hints,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    ModelWrapValidatorHandler,
    model_validator,
)
from pydantic._internal._generics import PydanticGenericMetadata
from pydantic._internal._model_construction import ModelMetaclass, NoInitField
from pydantic.fields import Field, FieldInfo, PrivateAttr
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import LoaderCallableStatus

from arcanum.association import Association
from arcanum.expression import Column

T = TypeVar("T", bound="BaseTransmuter")


class LoadedData: ...


validated = ContextVar("validated", default={})


@contextlib.contextmanager
def validation_context():
    token = validated.set({})

    try:
        yield
    finally:
        validated.reset(token)


@dataclass_transform(
    kw_only_default=True,
    field_specifiers=(Field, PrivateAttr, NoInitField),
)
class TransmuterMetaclass(ModelMetaclass):
    __provider__: type[Any]
    __transmuter_complete__: bool
    __transmuter_associations__: dict[str, FieldInfo]

    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        __pydantic_generic_metadata__: PydanticGenericMetadata | None = None,
        __pydantic_reset_parent_namespace__: bool = True,
        _create_model_module: str | None = None,
        **kwargs: Any,
    ) -> TransmuterMetaclass:
        cls: TransmuterMetaclass = super().__new__(
            mcs,
            cls_name,
            bases,
            namespace,
            __pydantic_generic_metadata__,
            __pydantic_reset_parent_namespace__,
            _create_model_module,
            **kwargs,
        )

        cls.__transmuter_associations__ = {}
        cls.__transmuter_associations_completed__ = False
        cls._ensure_associations_resolved()
        cls.__transmuter_complete__ = True

        return cls

    def _ensure_associations_resolved(self) -> None:
        if self.__transmuter_associations_completed__:
            return

        # Use get_type_hints to resolve all ForwardRefs at once
        try:
            resolved_hints = get_type_hints(self)
            self.__transmuter_associations_completed__ = True
        except (NameError, AttributeError):
            # Can't resolve all hints yet, fall back to manual checking
            resolved_hints = {}

        for name, info in self.__pydantic_fields__.items():
            if name in self.__transmuter_associations__:
                continue  # Already processed

            # Use the resolved type hint if available
            annotation = resolved_hints.get(name, info.annotation)

            # Check if it's an Association
            origin = get_origin(annotation)
            if origin:
                if isinstance(origin, type) and issubclass(origin, Association):
                    self.__transmuter_associations__[name] = info
            elif isinstance(annotation, type) and issubclass(annotation, Association):
                self.__transmuter_associations__[name] = info

    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getattr__(name)  # pyright: ignore[reportAttributeAccessIssue]
        except AttributeError as e:
            if object.__getattribute__(self, "__transmuter_complete__"):
                provider = object.__getattribute__(self, "__provider__")
                if hasattr(provider, name):
                    return getattr(provider, name)
            raise e

    @property
    def model_associations(self) -> dict[str, FieldInfo]:
        if not self.__transmuter_associations_completed__:
            self._ensure_associations_resolved()
        return self.__transmuter_associations__

    # TODO: No good way to give proper generic type to Column here
    def __getitem__(self, item: str) -> Column[Any]:
        if info := self.__pydantic_fields__.get(item):
            # false positive from pyright here
            # type[BaseTransmuter] or its subclasses are instances of the metaclass TransmuterMetaclass here
            column = Column[info.annotation](self, item, info)  # pyright: ignore[reportArgumentType]
            column.__args__ = (info.annotation,)
            return column
        raise KeyError(f"Field '{item}' not found in {self.__name__}")


class BaseTransmuter(BaseModel, ABC, metaclass=TransmuterMetaclass):
    __provider__: ClassVar[type[Any]]
    __provided__: Any = NoInitField(init=False)

    model_config = ConfigDict(from_attributes=True)

    def __getattribute__(self, name: str) -> Any:
        value: Any = object.__getattribute__(self, name)
        if name in type(self).model_associations and isinstance(value, Association):
            value.prepare(self, name)
        return value

    def __getattr__(self, name: str) -> Any:
        # only called when attribute not found in normal places
        try:
            return super().__getattr__(name)  # pyright: ignore[reportAttributeAccessIssue]
        except AttributeError as e:
            if hasattr(self.__provided__, name):
                return getattr(self.__provided__, name)
            raise e

    def __setattr__(self, name: str, value: Any):
        super().__setattr__(name, value)
        if self.__provided__ and name in type(self).model_fields:
            setattr(self.__provided__, name, getattr(self, name))

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.__provided__ = self.__provider__(**self.model_dump(exclude={"foo", "bar"}))
        for name in type(self).model_associations:
            association = getattr(self, name)
            if isinstance(association, Association):
                association.prepare(self, name)

    @model_validator(mode="wrap")
    @classmethod
    def model_formulate(
        cls, data: Any, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, cls.__provider__):
            context = validated.get()
            if cached := context.get(id(data)):
                return cached
            inspector = inspect(data)

            # don't use a dict to hold loaded data here
            # to avoid pydantic's handler call this formulate function again and go to the else block
            # use an object instead to keep the behavior same with pydantic's original model_validate
            # with from_attributes=True which will skip the instance __init__.
            loaded = LoadedData()

            # Get all loaded attributes from sqlalchemy orm instance
            for field_name, info in cls.model_fields.items():
                used_name = info.alias or field_name
                if used_name in inspector.attrs:
                    attr = inspector.attrs[used_name]
                    # skip unloaded attributes to prevent pydantic
                    # from firing the loadings on all lazy attributes in orm
                    setattr(loaded, used_name, attr.loaded_value)
                else:
                    # hybrid attrs maybe
                    setattr(loaded, used_name, LoaderCallableStatus.NO_VALUE)

            instance = handler(loaded)
            instance.__provided__ = data

            context[id(data)] = instance
        else:
            # normal initialization
            instance = handler(data)
        return instance
