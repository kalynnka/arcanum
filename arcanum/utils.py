from functools import lru_cache
from typing import Any

from pydantic import TypeAdapter


@lru_cache(maxsize=None)
def get_cached_adapter(type: type) -> TypeAdapter[Any]:
    """Cached TypeAdapter factory to avoid recreating adapters for the same type."""
    return TypeAdapter(type)
