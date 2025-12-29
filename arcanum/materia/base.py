from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, Self, TypeVar

from pydantic import ValidationInfo

if TYPE_CHECKING:
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
    def before_validator(materia: M, info: ValidationInfo) -> M:
        return materia

    @staticmethod
    def after_validator(transmuter: T, info: ValidationInfo) -> T:
        return transmuter


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
