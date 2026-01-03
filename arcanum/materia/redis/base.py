"""Redis Materia implementation for Arcanum.

Provides Redis-based caching coordination for Transmuter instances.
The actual IO logic is in RedisClient/AsyncRedisClient classes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationInfo

from arcanum.association import Association
from arcanum.materia.base import TM, BaseMateria

if TYPE_CHECKING:
    from arcanum.base import BaseTransmuter


class RedisMateria(BaseMateria):
    """Redis materia for coordinating caching of Transmuter instances.

    The materia itself only handles blessing and validation coordination.
    Actual caching IO is handled by RedisClient/AsyncRedisClient.
    """

    def __init__(self, key_prefix: str = "arcanum"):
        """Initialize RedisMateria.

        Args:
            key_prefix: Prefix for all Redis keys (default: "arcanum")
        """
        super().__init__()
        self.key_prefix = key_prefix
        # Store identifier fields for each blessed transmuter
        self._identifier_fields: dict[Any, str] = {}

    def bless(self, identifier_field: str = "id"):
        """Bless a transmuter with Redis caching capability.

        Args:
            identifier_field: Field name to use as the cache key (default: "id")
        """

        def decorator(transmuter_cls: TM) -> TM:
            if transmuter_cls in self.formulars:
                raise RuntimeError(
                    f"Transmuter {transmuter_cls.__name__} is already blessed with {self} in {self.__class__.__name__}"
                )
            # Store the identifier field name
            self._identifier_fields[transmuter_cls] = identifier_field
            # Mark transmuter as blessed
            self.formulars[transmuter_cls] = type(None)
            return transmuter_cls

        return decorator

    def transmuter_before_validator(
        self, transmuter_type: type[BaseTransmuter], materia: Any, info: ValidationInfo
    ) -> Any:
        """Pass through - actual cache checking is done in RedisClient."""
        return materia

    def transmuter_after_validator(
        self, transmuter: BaseTransmuter, info: ValidationInfo
    ) -> BaseTransmuter:
        """Pass through - actual caching is done in RedisClient."""
        return transmuter

    def load_association(self, association: Association) -> Any:
        """Associations are not cached in Redis."""
        raise NotImplementedError(
            "Redis materia does not support loading associations. "
            "Use RedisClient only for scalar field caching."
        )

    async def aload_association(self, association: Association) -> Any:
        """Associations are not cached in Redis."""
        raise NotImplementedError(
            "Redis materia does not support loading associations. "
            "Use RedisClient only for scalar field caching."
        )

    def get_identifier_field(self, transmuter_type: type[BaseTransmuter]) -> str | None:
        """Get the identifier field for a transmuter type."""
        return self._identifier_fields.get(transmuter_type)
