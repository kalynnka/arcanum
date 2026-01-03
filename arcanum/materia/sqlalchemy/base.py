from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from pydantic import ValidationInfo
from sqlalchemy import inspect
from sqlalchemy.exc import InvalidRequestError, MissingGreenlet
from sqlalchemy.orm import InstanceState

from arcanum.association import Association
from arcanum.base import BaseTransmuter
from arcanum.materia.base import TM, BaseMateria


class LoadedData: ...


class SqlalchemyMateria(BaseMateria):
    def bless(self, materia: type[Any]):
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

    def transmuter_before_validator(
        self, transmuter_type: type[BaseTransmuter], materia: Any, info: ValidationInfo
    ):
        inspector: InstanceState = inspect(materia)

        # don't use a dict to hold loaded data here
        # to avoid pydantic's handler call this formulate function again and go to the else block
        # use an object instead to keep the behavior same with pydantic's original model_validate
        # with from_attributes=True which will skip the instance __init__.
        loaded = LoadedData()

        # Get all loaded attributes from sqlalchemy orm instance
        # relationships/associations are excluded here to avoid circular validation
        # related objects will be validated when they are visited
        data = {}
        for field_name, field_info in transmuter_type.model_fields.items():
            if field_name in transmuter_type.model_associations:
                continue
            used_name = field_info.alias or field_name
            if used_name in inspector.attrs:
                data[used_name] = inspector.attrs[used_name].loaded_value

        loaded.__dict__ = data

        return loaded

    @staticmethod
    @contextmanager
    def association_load_context(association: Association) -> Generator[None]:
        try:
            yield
        except MissingGreenlet as missing_greenlet_error:
            association.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{association.field_name}' of {association.__instance__.__class__.__name__} for a greenlet is expected. Are you trying to get the relation in a sync context ? Await the {association.__instance__.__class__.__name__}.{association.field_name} instance to trigger the sqlalchemy async IO first."""
            ) from missing_greenlet_error
        except InvalidRequestError as invalid_request_error:
            association.__loaded__ = False
            raise RuntimeError(
                f"""Failed to load relation '{association.field_name}' of {association.__instance__.__class__.__name__} for the relation's loading strategy is set to 'raise' in sqlalchemy. Specify the relationship with selectinload in statement options or change the loading strategy to 'select' or 'selectin' instead."""
            ) from invalid_request_error
