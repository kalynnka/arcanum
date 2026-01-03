"""Redis client implementations for caching Transmuter instances."""

from __future__ import annotations

import contextlib
import json
from types import TracebackType
from typing import Any
from weakref import WeakKeyDictionary

from redis import Redis as RedisBase
from redis.asyncio import Redis as AsyncRedisBase

from arcanum.materia.base import active_materia
from arcanum.materia.redis.base import RedisMateria

ValidationContextT = WeakKeyDictionary[Any, Any]
ValidateContextGeneratorT = contextlib._GeneratorContextManager[
    ValidationContextT, None, None
]


class RedisClient(RedisBase):
    """Redis client for caching Transmuter instances.

    Extends Redis client with Transmuter caching capabilities.
    Handles all Redis IO operations for caching scalar fields.
    Relationships/associations are NOT cached.
    """

    cache_ttl: int | None
    _validation_context: ValidationContextT
    _validation_context_manager: ValidateContextGeneratorT | None

    def __init__(
        self,
        cache_ttl: int | None = None,
        **kwargs: Any,
    ):
        """Initialize RedisClient.

        Args:
            cache_ttl: Time-to-live in seconds for cached data (default: None - no expiration)
            **kwargs: Additional arguments passed to Redis constructor
        """
        super().__init__(**kwargs)
        self.cache_ttl = cache_ttl
        self._validation_context = WeakKeyDictionary()
        self._validation_context_manager = None

    @property
    def materia(self) -> RedisMateria:
        """Get the active RedisMateria from context.

        Returns:
            The active RedisMateria instance

        Raises:
            RuntimeError: If active materia is not a RedisMateria instance
        """
        materia = active_materia.get()
        if not isinstance(materia, RedisMateria):
            raise RuntimeError(
                f"RedisClient requires a RedisMateria context, got {type(materia).__name__}"
            )
        return materia

    def __enter__(self) -> "RedisClient":
        from arcanum.base import validation_context

        self._validation_context_manager = validation_context(self._validation_context)
        self._validation_context_manager.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._validation_context_manager:
            self._validation_context_manager.__exit__(exc_type, exc_val, exc_tb)
            self._validation_context_manager = None
        super().__exit__(exc_type, exc_val, exc_tb)

    def _make_key(self, transmuter_type: type, identifier: Any) -> str:
        """Generate Redis key for a transmuter instance."""
        return f"{self.materia.key_prefix}:{transmuter_type.__name__}:{identifier}"

    def get_cached(
        self, transmuter_type: type, identifier: Any
    ) -> dict[str, Any] | None:
        """Get cached data for a transmuter instance.

        Args:
            transmuter_type: Type of transmuter
            identifier: Value of the identifier field

        Returns:
            Cached data dict or None if not found
        """
        if transmuter_type not in self.materia.formulars:
            return None

        cache_key = self._make_key(transmuter_type, identifier)
        cached_data = super().get(cache_key)

        if cached_data:
            return json.loads(cached_data)  # type: ignore
        return None

    def set_cached(self, transmuter: Any) -> None:
        """Cache a transmuter instance.

        Only scalar fields are cached - associations are excluded.

        Args:
            transmuter: Transmuter instance to cache
        """
        transmuter_type = type(transmuter)
        identifier_field = self.materia.get_identifier_field(transmuter_type)

        if not identifier_field:
            return

        identifier_value = getattr(transmuter, identifier_field, None)
        if identifier_value is None:
            return

        # Cache scalar fields only (exclude associations)
        cache_data = transmuter.model_dump(
            exclude=set(transmuter_type.model_associations.keys()),
            mode="json",
        )

        cache_key = self._make_key(transmuter_type, identifier_value)

        if self.cache_ttl:
            super().setex(cache_key, self.cache_ttl, json.dumps(cache_data))
        else:
            super().set(cache_key, json.dumps(cache_data))

    def delete_cached(self, transmuter: Any) -> None:
        """Delete cached data for a transmuter instance.

        Args:
            transmuter: Transmuter instance to invalidate
        """
        transmuter_type = type(transmuter)
        identifier_field = self.materia.get_identifier_field(transmuter_type)

        if not identifier_field:
            return

        identifier_value = getattr(transmuter, identifier_field, None)
        if identifier_value is None:
            return

        cache_key = self._make_key(transmuter_type, identifier_value)
        super().delete(cache_key)

    def delete_all_cached(self, transmuter_type: type) -> int:
        """Delete all cached instances of a transmuter type.

        Args:
            transmuter_type: Type of transmuter

        Returns:
            Number of keys deleted
        """
        pattern = f"{self.materia.key_prefix}:{transmuter_type.__name__}:*"
        keys = list(self.scan_iter(match=pattern))
        if keys:
            return super().delete(*keys)  # type: ignore
        return 0

    def exists_cached(self, transmuter_type: type, identifier: Any) -> bool:
        """Check if a cached entry exists.

        Args:
            transmuter_type: Type of transmuter
            identifier: Value of the identifier field

        Returns:
            True if cached data exists
        """
        cache_key = self._make_key(transmuter_type, identifier)
        return bool(super().exists(cache_key))


class AsyncRedisClient(AsyncRedisBase):
    """Async Redis client for caching Transmuter instances.

    Extends async Redis client with Transmuter caching capabilities.
    Handles all async Redis IO operations for caching scalar fields.
    Relationships/associations are NOT cached.
    """

    cache_ttl: int | None
    _validation_context: ValidationContextT
    _validation_context_manager: ValidateContextGeneratorT | None

    def __init__(
        self,
        cache_ttl: int | None = None,
        **kwargs: Any,
    ):
        """Initialize AsyncRedisClient.

        Args:
            cache_ttl: Time-to-live in seconds for cached data (default: None - no expiration)
            **kwargs: Additional arguments passed to async Redis constructor
        """
        super().__init__(**kwargs)
        self.cache_ttl = cache_ttl
        self._validation_context = WeakKeyDictionary()
        self._validation_context_manager = None

    @property
    def materia(self) -> RedisMateria:
        """Get the active RedisMateria from context.

        Returns:
            The active RedisMateria instance

        Raises:
            RuntimeError: If active materia is not a RedisMateria instance
        """
        materia = active_materia.get()
        if not isinstance(materia, RedisMateria):
            raise RuntimeError(
                f"AsyncRedisClient requires a RedisMateria context, got {type(materia).__name__}"
            )
        return materia

    async def __aenter__(self) -> "AsyncRedisClient":
        from arcanum.base import validation_context

        self._validation_context_manager = validation_context(self._validation_context)
        self._validation_context_manager.__enter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._validation_context_manager:
            self._validation_context_manager.__exit__(exc_type, exc_val, exc_tb)
            self._validation_context_manager = None
        await super().__aexit__(exc_type, exc_val, exc_tb)

    def _make_key(self, transmuter_type: type, identifier: Any) -> str:
        """Generate Redis key for a transmuter instance."""
        return f"{self.materia.key_prefix}:{transmuter_type.__name__}:{identifier}"

    async def get_cached(
        self, transmuter_type: type, identifier: Any
    ) -> dict[str, Any] | None:
        """Get cached data for a transmuter instance.

        Args:
            transmuter_type: Type of transmuter
            identifier: Value of the identifier field

        Returns:
            Cached data dict or None if not found
        """
        if transmuter_type not in self.materia.formulars:
            return None

        cache_key = self._make_key(transmuter_type, identifier)
        cached_data = await super().get(cache_key)

        if cached_data:
            return json.loads(cached_data)
        return None

    async def set_cached(self, transmuter: Any) -> None:
        """Cache a transmuter instance.

        Only scalar fields are cached - associations are excluded.

        Args:
            transmuter: Transmuter instance to cache
        """
        transmuter_type = type(transmuter)
        identifier_field = self.materia.get_identifier_field(transmuter_type)

        if not identifier_field:
            return

        identifier_value = getattr(transmuter, identifier_field, None)
        if identifier_value is None:
            return

        # Cache scalar fields only (exclude associations)
        cache_data = transmuter.model_dump(
            exclude=set(transmuter_type.model_associations.keys()),
            mode="json",
        )

        cache_key = self._make_key(transmuter_type, identifier_value)

        if self.cache_ttl:
            await super().setex(cache_key, self.cache_ttl, json.dumps(cache_data))
        else:
            await super().set(cache_key, json.dumps(cache_data))

    async def delete_cached(self, transmuter: Any) -> None:
        """Delete cached data for a transmuter instance.

        Args:
            transmuter: Transmuter instance to invalidate
        """
        transmuter_type = type(transmuter)
        identifier_field = self.materia.get_identifier_field(transmuter_type)

        if not identifier_field:
            return

        identifier_value = getattr(transmuter, identifier_field, None)
        if identifier_value is None:
            return

        cache_key = self._make_key(transmuter_type, identifier_value)
        await super().delete(cache_key)

    async def delete_all_cached(self, transmuter_type: type) -> int:
        """Delete all cached instances of a transmuter type.

        Args:
            transmuter_type: Type of transmuter

        Returns:
            Number of keys deleted
        """
        pattern = f"{self.materia.key_prefix}:{transmuter_type.__name__}:*"
        keys = []
        async for key in self.scan_iter(match=pattern):
            keys.append(key)
        if keys:
            return await super().delete(*keys)
        return 0

    async def exists_cached(self, transmuter_type: type, identifier: Any) -> bool:
        """Check if a cached entry exists.

        Args:
            transmuter_type: Type of transmuter
            identifier: Value of the identifier field

        Returns:
            True if cached data exists
        """
        cache_key = self._make_key(transmuter_type, identifier)
        return bool(await super().exists(cache_key))
