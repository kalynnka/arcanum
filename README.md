# Arcanum

**Arcanum** is a Python library designed to seamlessly bind Pydantic schemas with various datasources, eliminating the need to manually create templates, factories, and utilities repeatedly. It provides a unified interface for working with different data backends while maintaining type safety and validation through Pydantic.

> **⚠️ Warning:** This repository is still a work in progress and is currently at a minimum viable state. Expect bugs, breaking changes, and incomplete features. Use at your own risk!

> **⚠️ Note:** At the moment, SQLAlchemy is the only supported provider and is hardcoded as the default backend.

## Quick Start

> **⚠️ Important:** When using the SQLAlchemy provider, you must use `arcanum.database.Session` instead of SQLAlchemy's native `sqlalchemy.orm.Session`. The arcanum Session handles the automatic "blessing" of ORM objects into transmuter schemas. Using SQLAlchemy's Session directly will not perform this conversion automatically.

### Define Your ORM Models

First, define your SQLAlchemy ORM models as usual:

```python
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase): ...


class Foo(Base):
    __tablename__ = "foo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)

    bars: Mapped[list["Bar"]] = relationship(
        back_populates="foo", uselist=True, cascade="all, delete-orphan"
    )


class Bar(Base):
    __tablename__ = "bar"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data: Mapped[str] = mapped_column(String(50), nullable=False)
    foo_id: Mapped[int] = mapped_column(
        ForeignKey(Foo.id, ondelete="CASCADE"),
        nullable=False,
    )
    foo: Mapped[Foo] = relationship(back_populates="bars", uselist=False)
```

### Define Your Transmuter Schemas

Create Pydantic-based schemas that bind to your ORM models using `BaseTransmuter`:

```python
from typing import Annotated, ClassVar, Optional

from pydantic import Field
from pydantic._internal._model_construction import NoInitField

from arcanum.association import Relation, RelationCollection
from arcanum.base import BaseTransmuter, Identity
from models import Bar as BarModel
from models import Foo as FooModel


class Foo(BaseTransmuter):
    __provider__: ClassVar[type[FooModel]] = FooModel
    __provided__: FooModel = NoInitField(init=False)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    name: str
    bars: RelationCollection[Bar] = Field(default=RelationCollection())


class Bar(BaseTransmuter):
    __provider__: ClassVar[type[BarModel]] = BarModel
    __provided__: BarModel = NoInitField(init=False)

    id: Annotated[Optional[int], Identity] = Field(default=None, frozen=True)
    data: str
    foo_id: int | None = None

    foo: Relation[Foo] = Field(default=Relation())
```

### Using Schemas Like ORM Objects

Once defined, you can use your transmuter schemas just like ORM objects with SQLAlchemy statements:

```python
from sqlalchemy import select, insert, update, delete

from arcanum.database import Session

# Create and add objects
with Session(engine) as session:
    foo = Foo(name="My Foo")
    bar = Bar(data="Child Bar", foo=Relation(foo))
    session.add(bar)
    session.flush()
    
    # Sync server-generated values (like autoincrement IDs) to transmuter
    # For dialects that support RETURNING (PostgreSQL, SQLite, etc.):
    foo.revalidate()  # Syncs ORM state to transmuter, no extra query
    bar.revalidate()
    
    # For dialects without RETURNING support (MySQL 😒):
    session.refresh(foo)  # Issues additional SELECT statements
    session.refresh(bar)
    
    session.commit()

# Query using schemas directly in SQLAlchemy statements
with Session(engine) as session:
    # Use schema fields in where clauses with bracket notation
    stmt = select(Foo).where(Foo["id"] == 1)
    foo = session.execute(stmt).scalars().one()
    
    # Get single objects by primary key
    foo = session.get_one(Foo, 1)

# Access relationships
with Session(engine) as session:
    foo = session.get_one(Foo, 1)
    
    for bar in foo.bars:
        print(bar.data)
        print(bar.foo.value)  # Access parent via Relation

# Insert with returning
with Session(engine) as session:
    stmt = insert(Foo).values(name="New Foo").returning(Foo)
    result = session.execute(stmt)
    new_foo = result.scalars().one()

# Update with returning
with Session(engine) as session:
    stmt = (
        update(Foo)
        .where(Foo["id"] == 1)
        .values(name="Updated Foo")
        .returning(Foo)
    )
    result = session.execute(stmt)
    updated_foo = result.scalars().one()

# Delete with returning
with Session(engine) as session:
    stmt = delete(Foo).where(Foo["id"] == 1).returning(Foo)
    result = session.execute(stmt)
    deleted_foo = result.scalars().one()
```

### Partial Models for Create/Update Operations

Arcanum provides `Create` and `Update` partial model helpers:

```python
# Create partial (excludes identity fields)
partial = Foo.Create(name="Partial Foo")
foo = Foo.shell(partial)  # Create a full schema instance

# Update partial (respects frozen fields)
partial = Foo.Update(name="Updated Name")
foo = Foo(id=2, name="Initial Name").absorb(partial)
# foo.name is now "Updated Name", foo.id remains 2
```

### Accessing Relationships

Arcanum uses `Relation` and `RelationCollection` to handle relationships between schemas:

```python
with Session(engine) as session:
    foo = session.get_one(Foo, 1)
    
    # Access one-to-many relationships via RelationCollection
    for bar in foo.bars:
        print(bar.data)
        
        # Access many-to-one relationship via Relation.value
        parent_foo = bar.foo.value
        assert parent_foo is foo  # Same object reference
```

When creating objects with relationships, you can wrap related objects in `Relation`. 
It works fine without wrapping, but the wrapper keeps your type checker from complaining 😏:

```python
with Session(engine) as session:
    foo = Foo(name="Parent Foo")
    
    # With Relation wrapper (recommended for type safety)
    bar = Bar(data="Child Bar", foo=Relation(foo))
    
    # Without wrapper (also works, but type checkers may complain)
    bar = Bar(data="Child Bar", foo=foo)
    
    session.add(bar)
    session.flush()
    session.commit()
```

### Session Helper Methods

The arcanum `Session` provides convenient helper methods for common query patterns:

#### `get` / `get_one`

Retrieve a single object by primary key:

```python
with Session(engine) as session:
    # Returns None if not found
    foo = session.get(Foo, 1)
    
    # Raises NoResultFound if not found
    foo = session.get_one(Foo, 1)
```

#### `one` / `one_or_none`

Query for a single result with filters:

```python
with Session(engine) as session:
    # Using keyword filters
    foo = session.one(Foo, name="My Foo")
    
    # Using expressions with bracket notation
    foo = session.one(Foo, expressions=[Foo["name"] == "My Foo"])
    
    # Returns None instead of raising if not found
    foo = session.one_or_none(Foo, name="Maybe Exists")
```

#### `first`

Get the first result with optional ordering:

```python
with Session(engine) as session:
    foo = session.first(Foo, order_bys=[Foo["name"]])
```

#### `list`

Retrieve multiple objects with pagination:

```python
with Session(engine) as session:
    # Get up to 100 items (default limit)
    foos = session.list(Foo)
    
    # With pagination
    foos = session.list(Foo, limit=10, offset=20)
    
    # With filters and ordering
    foos = session.list(
        Foo,
        expressions=[Foo["name"].like("Test%")],
        order_bys=[Foo["id"].desc()],
        limit=50
    )
```

#### `bulk`

Efficiently retrieve multiple objects by their primary keys:

```python
with Session(engine) as session:
    # Returns list in same order as idents, with None for missing
    foos = session.bulk(Foo, [1, 2, 3, 4, 5])
```

#### `count`

Count matching rows:

```python
with Session(engine) as session:
    total = session.count(Foo)
    filtered = session.count(Foo, expressions=[Foo["name"].like("Test%")])
```

#### `partitions`

Stream large result sets in chunks:

```python
with Session(engine) as session:
    for partition in session.partitions(Foo, size=100):
        for foo in partition:
            process(foo)
```




