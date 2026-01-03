from __future__ import annotations

# Redis Materia test fixtures
# This module contains shared fixtures for testing the Redis materia implementation.
# Includes RedisMateria setup and Redis client configuration.
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio

from arcanum.materia.redis import AsyncRedisClient, RedisClient, RedisMateria

# Redis URL points to the docker-compose redis service exposed on localhost
REDIS_URL = "redis://localhost:6379/0"


@pytest.fixture(scope="session")
def redis_materia() -> RedisMateria:
    """Create a RedisMateria instance for testing.

    Returns:
        RedisMateria instance with test key prefix
    """
    return RedisMateria(key_prefix="test")


@pytest.fixture(scope="function")
def redis_client(redis_materia: RedisMateria) -> Generator[RedisClient, None, None]:
    """Create a sync RedisClient for testing.

    Args:
        redis_materia: RedisMateria fixture

    Returns:
        RedisClient instance connected to test Redis
    """
    with redis_materia:
        client = RedisClient(host="localhost", port=6379, db=0, decode_responses=True)
        try:
            yield client
        finally:
            client.close()


@pytest_asyncio.fixture(scope="function")
async def async_redis_client(
    redis_materia: RedisMateria,
) -> AsyncGenerator[AsyncRedisClient, None]:
    """Create an async RedisClient for testing.

    Args:
        redis_materia: RedisMateria fixture

    Returns:
        AsyncRedisClient instance connected to test Redis
    """
    with redis_materia:
        client = AsyncRedisClient(
            host="localhost", port=6379, db=0, decode_responses=True
        )
        try:
            yield client
        finally:
            await client.aclose()
