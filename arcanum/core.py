from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar, Self, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    ModelWrapValidatorHandler,
    model_validator,
)
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import LoaderCallableStatus

T = TypeVar("T", bound="BaseProtocol")


class LoadedData: ...


class BaseProtocol(BaseModel, ABC):
    __provider__: ClassVar[type[Any]]
    __provided__: Any

    model_config = ConfigDict(from_attributes=True)

    def __setattr__(self, name: str, value: Any):
        super().__setattr__(name, value)
        setattr(self.__provided__, name, getattr(self, name))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, BaseProtocol):
            return False
        return self.__provided__ is other.__provided__

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.__provided__ = self.__provider__(**self.model_dump())

    @model_validator(mode="wrap")
    @classmethod
    def model_formulate(
        cls, data: Any, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, cls.__provider__):
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
        else:
            # normal initialization
            instance = handler(data)
        return instance
