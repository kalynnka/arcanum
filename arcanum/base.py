from __future__ import annotations

import contextlib
from abc import ABC
from contextvars import ContextVar
from typing import Any, ClassVar, Optional, Self, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    ModelWrapValidatorHandler,
    model_validator,
)
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import LoaderCallableStatus

from arcanum.association import Association

T = TypeVar("T", bound="BaseProtocol")


class LoadedData: ...


validated = ContextVar("validated", default={})


@contextlib.contextmanager
def validation_context():
    token = validated.set({})

    try:
        yield
    finally:
        validated.reset(token)


class BaseProtocol(BaseModel, ABC):
    __provider__: ClassVar[type[Any]]
    __provided__: Any | None = None

    model_config = ConfigDict(from_attributes=True)

    def __getattribute__(self, name: str) -> Any:
        value: Any = object.__getattribute__(self, name)
        if isinstance(value, Association):
            value.prepare(self, name)
        return value  # type: ignore

    def __setattr__(self, name: str, value: Any):
        super().__setattr__(name, value)
        if self.__provided__ and name in type(self).model_fields:
            setattr(self.__provided__, name, getattr(self, name))

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.__provided__ = self.__provided__ or self.__provider__(
            **self.model_dump(exclude={"foo", "bar"})
        )
        for field_name, value in self:
            if isinstance(value, Association):
                value.prepare(self, field_name)

    @property
    def metadata(self) -> Optional[Any]:
        return self.__provided__.metadata if self.__provided__ else None

    @property
    def __mapper__(self) -> Optional[Any]:
        return self.__provided__.__mapper__ if self.__provided__ else None

    @property
    def __table__(self) -> Optional[Any]:
        return self.__provided__.__table__ if self.__provided__ else None

    @property
    def __tablename__(self) -> Optional[Any]:
        return self.__provided__.__tablename__ if self.__provided__ else None

    @property
    def __sa_registry__(self) -> Optional[Any]:
        return self.__provided__.__sa_registry__ if self.__provided__ else None

    @property
    def _sa_class_manager(self) -> Optional[Any]:
        return self.__provided__._sa_class_manager if self.__provided__ else None

    @property
    def _sa_instance_state(self) -> Optional[Any]:
        return self.__provided__._sa_instance_state if self.__provided__ else None

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
