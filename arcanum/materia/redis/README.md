# Redis Materia

Redis-based caching layer for Arcanum Transmuter instances.

## Features

- **Sync and Async Support**: Provides both `RedisMateria` (sync) and `AsyncRedisMateria` (async) implementations
- **Scalar Field Caching**: Caches all scalar fields of Transmuter instances
- **No Relationship Caching**: Relationships/associations are explicitly excluded from cache to avoid complex serialization issues
- **Configurable TTL**: Optional time-to-live for cached entries
- **Key Prefix**: Configurable key prefix for namespace isolation
- **Cache Invalidation**: Methods to invalidate individual instances or all instances of a type

## Installation

Requires `redis` package:

```bash
pip install redis
```

## Usage

### Sync Redis

```python
from redis import Redis
from arcanum.materia.redis import RedisMateria

# Create Redis client
redis_client = Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Create materia with optional TTL (in seconds)
redis_materia = RedisMateria(redis_client, key_prefix="myapp", ttl=3600)

# Bless transmuters (using 'id' field as cache key)
@redis_materia.bless(identifier_field="id")
class User(BaseTransmuter):
    id: int
    name: str
    email: str
    # relationships are not cached
    posts: Collection[Post] = Collection()

# Use within context
with redis_materia:
    user = User(id=1, name="Alice", email="alice@example.com")
    # User is cached with key: "myapp:User:1"
    
    # Load from cache on subsequent access
    user2 = User(id=1)  # Will check cache first
```

### Async Redis

```python
from redis.asyncio import Redis
from arcanum.materia.redis import AsyncRedisMateria

# Create async Redis client
redis_client = Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Create materia
redis_materia = AsyncRedisMateria(redis_client, key_prefix="myapp", ttl=3600)

@redis_materia.bless(identifier_field="id")
class User(BaseTransmuter):
    id: int
    name: str
    email: str

# Use within async context
async with redis_materia:
    user = User(id=1, name="Alice", email="alice@example.com")
    # Use async methods for cache operations
    await redis_materia.transmuter_after_validator_async(user, info)
```

## Cache Invalidation

```python
# Invalidate single instance
redis_materia.invalidate(user)

# Invalidate all users
redis_materia.invalidate_all(User)
```

## Docker Compose

A Redis service is provided in `docker-compose.yml`:

```bash
docker-compose up redis
```

## Important Notes

1. **Relationships Are Not Cached**: Only scalar fields are cached to Redis. Relationships must be loaded separately.

2. **Identifier Field**: The field specified in `bless(identifier_field="...")` is used as the cache key. It should be unique and immutable.

3. **JSON Serialization**: Data is serialized using `model_dump(mode="json")`, so custom types must be JSON-serializable.

4. **Cache-Aside Pattern**: The materia implements a cache-aside pattern - data is loaded from the primary source and cached, not read-through.

5. **TTL**: If not specified, cached entries never expire. Consider setting appropriate TTL values for your use case.
