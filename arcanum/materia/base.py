from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generator,
    Generic,
    Optional,
    Self,
    TypeVar,
)

from pydantic import ValidationInfo
from sqlalchemy.exc import InvalidRequestError, MissingGreenlet

if TYPE_CHECKING:
    from arcanum.association import Association
    from arcanum.base import BaseTransmuter, TransmuterMetaclass

M = TypeVar("M", bound=Any)
T = TypeVar("T", bound="BaseTransmuter")
TM = TypeVar("TM", bound="TransmuterMetaclass")

K = TypeVar("K")
V = TypeVar("V")


class BidirectonDict(dict, Generic[K, V]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._reverse: BidirectonDict[V, K] | None = None

    @property
    def reverse(self) -> BidirectonDict[V, K]:
        if self._reverse is None:
            self._reverse = BidirectonDict({v: k for k, v in self.items()})
            self._reverse._reverse = self
        return self._reverse

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        if self._reverse is not None:
            dict.__setitem__(self._reverse, value, key)

    def __delitem__(self, key: K) -> None:
        value = self[key]
        super().__delitem__(key)
        if self._reverse is not None:
            dict.__delitem__(self._reverse, value)


class BaseMateria:
    formulars: BidirectonDict[TransmuterMetaclass, type[Any]]
    active_tokens: list[Token[BaseMateria]]

    def __init__(self) -> None:
        self.formulars = BidirectonDict()
        self.active_tokens = []

    def __enter__(self) -> Self:
        token = active_materia.set(self)
        self.active_tokens.append(token)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        token = self.active_tokens.pop()
        active_materia.reset(token)

    def __getitem__(self, transmuter: TransmuterMetaclass) -> type[Any] | None:
        return self.formulars.get(transmuter)

    def __contains__(self, transmuter: TransmuterMetaclass) -> bool:
        return transmuter in self.formulars

    def bless(self, materia: Any):
        def decorator(transmuter_cls: TM) -> TM:
            self.formulars[transmuter_cls] = materia
            return transmuter_cls

        return decorator

    @staticmethod
    def before_validator(
        transmuter_type: type[T], materia: M, info: ValidationInfo
    ) -> M:
        return materia

    @staticmethod
    def after_validator(transmuter: T, info: ValidationInfo) -> T:
        return transmuter

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


class NoOpMateria(BaseMateria):
    _instance: ClassVar[Optional[NoOpMateria]] = None
    _initialized: ClassVar[bool] = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not NoOpMateria._initialized:
            super().__init__()
            NoOpMateria._initialized = True

    def bless(self):
        def decorator(transmuter_cls: TM) -> TM:
            # No operation performed, just return the class as is
            return transmuter_cls

        return decorator


active_materia: ContextVar[BaseMateria] = ContextVar(
    "active_materia", default=NoOpMateria()
)
