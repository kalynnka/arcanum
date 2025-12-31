from typing import Any

from pydantic import ValidationInfo
from sqlalchemy import inspect
from sqlalchemy.orm import InstanceState

from arcanum.materia.base import TM, BaseMateria


class LoadedData: ...


class SqlalchemyMateria(BaseMateria):
    def before_validator(self, materia: Any, info: ValidationInfo):
        inspector: InstanceState = inspect(materia)

        # don't use a dict to hold loaded data here
        # to avoid pydantic's handler call this formulate function again and go to the else block
        # use an object instead to keep the behavior same with pydantic's original model_validate
        # with from_attributes=True which will skip the instance __init__.
        loaded = LoadedData()

        # Get all loaded attributes from sqlalchemy orm instance
        # skip unloaded attributes to prevent pydantic
        # from firing the loadings on all lazy attributes in orm
        loaded.__dict__ = {
            name: attr.loaded_value for name, attr in inspector.attrs.items()
        }

        return loaded

    def bless(self, materia: Any):
        def decorator(transmuter_cls: TM) -> TM:
            if transmuter_cls in self.formulars:
                raise RuntimeError(
                    f"Transmuter {transmuter_cls.__name__} is already blessed with {self} in {self.__class__.__name__}"
                )
            # Check if materia implements TransmuterProxied by verifying required attributes
            if not hasattr(materia, "transmuter_proxy"):
                raise TypeError(
                    "SQLAlchemyMateria require materia must implement TransmuterProxied."
                )
            self.formulars[transmuter_cls] = materia
            return transmuter_cls

        return decorator
