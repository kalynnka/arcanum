from __future__ import annotations

import contextlib
from abc import ABC
from contextvars import ContextVar
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
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
    create_model,
    model_validator,
)
from pydantic._internal._model_construction import ModelMetaclass, NoInitField
from pydantic.fields import Field, FieldInfo, PrivateAttr
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import LoaderCallableStatus

from arcanum.association import Association
from arcanum.expression import Column

T = TypeVar("T", bound="BaseTransmuter")
M = TypeVar("M", bound="TransmuterMetaclass")


class LoadedData: ...


validated: ContextVar[dict[Any, BaseTransmuter]] = ContextVar("validated", default={})


@contextlib.contextmanager
def validation_context():
    context: dict[Any, BaseTransmuter] = {}
    token = validated.set(context)
    try:
        yield context
    finally:
        validated.reset(token)


class Identity:
    """Marker class for identity fields that could not be set in creation and immutable."""


@dataclass_transform(
    kw_only_default=True,
    field_specifiers=(Field, PrivateAttr, NoInitField),
)
class TransmuterMetaclass(ModelMetaclass):
    __transmuter_registry__: ClassVar[dict[type[Any], TransmuterMetaclass]] = {}

    __transmuter_complete__: bool
    __transmuter_associations__: dict[str, FieldInfo]
    __transmuter_identities__: dict[str, FieldInfo]
    __transmuter_create_model__: Optional[type[BaseModel]]
    __transmuter_update_model__: Optional[type[BaseModel]]

    if TYPE_CHECKING:
        __pydantic_fields__: dict[str, FieldInfo]

        model_config: ConfigDict
        model_fields: dict[str, FieldInfo]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.__transmuter_associations__ = {}
        self.__transmuter_associations_completed__ = False
        self.__transmuter_identities__ = {}
        self.__transmuter_create_model__ = None
        self.__transmuter_update_model__ = None

        self._ensure_associations_resolved()

        for name, info in self.__pydantic_fields__.items():
            for metadata in info.metadata:
                if isinstance(metadata, type) and issubclass(metadata, Identity):
                    self.__transmuter_identities__[name] = info
                    break
                elif isinstance(metadata, Identity):
                    self.__transmuter_identities__[name] = info
                    break

        self.__transmuter_complete__ = True

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

    # TODO: No good way to give proper generic type to Column here
    def __getitem__(self, item: str) -> Column[Any]:
        if info := self.__pydantic_fields__.get(item):
            # false positive from pyright here
            # type[BaseTransmuter] or its subclasses are instances of the metaclass TransmuterMetaclass here
            column = Column[info.annotation](self, item, info)  # pyright: ignore[reportArgumentType]
            column.__args__ = (info.annotation,)
            return column
        raise KeyError(f"Field '{item}' not found in {self.__name__}")

    @property
    def model_associations(self) -> dict[str, FieldInfo]:
        if not self.__transmuter_associations_completed__:
            self._ensure_associations_resolved()
        return self.__transmuter_associations__

    @property
    def model_identities(self) -> dict[str, FieldInfo]:
        return self.__transmuter_identities__

    @property
    def model_registry(self) -> dict[type[Any], TransmuterMetaclass]:
        return self.__transmuter_registry__

    @property
    def Create(self) -> type[BaseModel]:
        if self.__transmuter_create_model__:
            return self.__transmuter_create_model__

        config = self.model_config.copy()

        field_definitions: dict[str, tuple[Any, FieldInfo]] = {}
        # TODO: include nested associations
        for field_name in set(
            self.__pydantic_fields__.keys()
            - self.model_identities.keys()
            - set(self.model_associations.keys())  # TODO: include nested associations
        ):
            info = self.__pydantic_fields__[field_name]
            field_definitions[field_name] = (info.annotation, info)

        self.__transmuter_create_model__ = create_model(
            f"{self.__name__}Create",
            __config__=config,
            __module__=self.__module__,
            **field_definitions,  # type: ignore
        )

        return self.__transmuter_create_model__  # type: ignore

    @property
    def Update(self) -> type[BaseModel]:
        if self.__transmuter_update_model__:
            return self.__transmuter_update_model__

        config = self.model_config.copy()

        field_definitions: dict[str, tuple[Any, FieldInfo]] = {}
        # TODO: include nested associations
        for field_name in set(
            self.__pydantic_fields__.keys() - set(self.model_associations.keys())
        ):
            info = self.__pydantic_fields__[field_name]
            if not info.frozen:
                field_definitions[field_name] = (Optional[info.annotation], info)

        self.__transmuter_update_model__ = create_model(
            f"{self.__name__}Update",
            __config__=config,
            __module__=self.__module__,
            **field_definitions,  # type: ignore
        )
        return self.__transmuter_update_model__  # type: ignore


class BaseTransmuter(BaseModel, ABC, metaclass=TransmuterMetaclass):
    __provider__: ClassVar[type[Any]]
    __provided__: Any = NoInitField(init=False)

    _provider_revalidating: bool = PrivateAttr(default=False)

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
        self.__provided__ = self.__provider__(
            **self.model_dump(exclude=set(type(self).model_associations.keys()))
        )
        for name in type(self).model_associations:
            association = getattr(self, name)
            if isinstance(association, Association):
                association.prepare(self, name)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "__provider__") and cls.__provider__ is not None:
            cls.__transmuter_registry__[cls.__provider__] = cls
        else:
            raise ValueError(
                f"Transmuter subclass {cls.__name__} must define a __provider__"
            )

    @classmethod
    def __clause_element__(cls):
        return inspect(cls.__provider__)

    @model_validator(mode="wrap")
    @classmethod
    def model_formulate(
        cls, data: Any, handler: ModelWrapValidatorHandler[Self]
    ) -> BaseTransmuter:
        if isinstance(data, cls.__provider__):
            context = validated.get()
            if cached := context.get(data):
                # if the cached instance is in revalidating state, let it through to sync orm state
                if not cached._provider_revalidating:
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

            if not cached:
                context[data] = instance
        else:
            # normal initialization
            instance = handler(data)
        return instance

    @classmethod
    def shell(cls, create_partial: BaseModel) -> Self:
        """Create a new instance using the Create partial model. No good way to do proper typing for the input data"""
        partial = cls.Create.model_validate(create_partial)
        return cls(**partial.model_dump())

    def absorb(self, update_partial: BaseModel) -> Self:
        """Update the instance using the Update partial model."""
        partial = (
            type(self)
            .Update.model_validate(update_partial)
            .model_dump(exclude_unset=True)
        )
        for key, value in partial.items():
            setattr(self, key, value)
        return self

    def revalidate(self) -> Self:
        """Re-validate the instance against the underlying provider instance."""
        # if True, it means that the revalidation is already in progress and triggered by an upper validation round,
        # so we skip the revalidation here to avoid infinite recursion.
        if self._provider_revalidating:
            return self

        self._provider_revalidating = True
        if self.__provided__:
            self.__pydantic_validator__.validate_python(
                self.__provided__,
                self_instance=self,
            )
        # double ensure the revalidation flag is reset to False
        self._provider_revalidating = False

        return self
