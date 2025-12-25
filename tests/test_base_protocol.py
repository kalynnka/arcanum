import pytest
from pydantic import ValidationError
from sqlalchemy import Engine

from arcanum.database import Session
from arcanum.dml import delete, insert, update
from arcanum.selectable import select
from tests.models import Foo as FooModel
from tests.schemas import Bar, Foo


def test_bless_foo_into_protocol(foo_with_bar: Foo, engine: Engine):
    with Session(engine) as session:
        foo = session.get(Foo, foo_with_bar.id)

        assert foo is not None
        assert foo.id == foo_with_bar.id
        assert foo.name == foo_with_bar.name
        assert isinstance(foo.__provided__, FooModel)

        assert len(foo.bars) == 2
        assert isinstance(foo.bars[0], Bar)
        assert isinstance(foo.bars[0].foo.value, Foo)
        assert foo.bars[0].foo.value is foo

        foo.bars[0].data = "Updated Bar Data"
        session.flush()

        bar = session.get(Bar, foo.bars[0].id)
        assert bar is not None
        assert bar.data == foo.bars[0].data


def test_column_expression(engine: Engine, foo_with_bar: Foo):
    stmt = select(Foo).where(Foo["id"] == foo_with_bar.id)
    with Session(engine) as session:
        result = session.execute(stmt)
        foo = result.scalars().one()
        assert foo is not None
        assert foo.id == foo_with_bar.id
        assert foo.name == foo_with_bar.name


def test_adapted_select(engine: Engine, foo_with_bar: Foo):
    with Session(engine) as session:
        stmt = select(Foo, Foo.name).where(Foo["id"] == foo_with_bar.id)
        result = session.execute(stmt)
        row = result.fetchone()
        assert row
        assert isinstance(row[0], Foo)
        assert row[0].id == foo_with_bar.id
        assert row[0].name == foo_with_bar.name
        assert row[1] == foo_with_bar.name


def test_adapted_insert(engine: Engine):
    with Session(engine) as session:
        # no returning
        stmt = insert(Foo).values(name="Inserted Foo")
        session.execute(stmt)

        inserted_foo = (
            session.execute(select(Foo).where(Foo["name"] == "Inserted Foo"))
            .scalars()
            .one()
        )

        assert inserted_foo is not None
        assert inserted_foo.name == "Inserted Foo"

        # with returning
        stmt = insert(Foo).values(name="Inserted Foo With Returning").returning(Foo)
        result = session.execute(stmt)
        inserted_foo = result.scalars().one()
        assert result


def test_adapted_update(engine: Engine, foo_with_bar: Foo):
    with Session(engine) as session:
        foo_id = foo_with_bar.id

        # update without returning
        stmt = update(Foo).where(Foo["id"] == foo_id).values(name="Updated Foo")
        session.execute(stmt)

        updated = session.get(Foo, foo_id)
        assert updated
        assert updated.name == "Updated Foo"

        # update with returning
        stmt = (
            update(Foo)
            .where(Foo["id"] == foo_id)
            .values(name="Updated Foo With Returning")
            .returning(Foo)
        )
        result = session.execute(stmt)
        updated_foo = result.scalars().one()

        assert isinstance(updated_foo, Foo)
        assert updated_foo.id == foo_id
        assert updated_foo.name == "Updated Foo With Returning"


def test_adapted_delete(engine: Engine, foo_with_bar: Foo):
    with Session(engine) as session:
        # Create another foo to delete
        new_foo = Foo(name="To Be Deleted")
        session.add(new_foo)
        session.flush()
        # TODO: we must refresh/re-validate here to sync the returned identity keys from orm to transmuter object
        session.refresh(new_foo)

        # Or
        # new_foo.revalidate()

        # delete with returning
        stmt = delete(Foo).where(Foo["id"] == new_foo.id).returning(Foo)
        result = session.execute(stmt)
        deleted_foo = result.scalars().one()

        assert isinstance(deleted_foo, Foo)
        assert deleted_foo.id == new_foo.id
        assert deleted_foo.name == "To Be Deleted"

        # verify it's actually deleted
        after = (
            session.execute(select(Foo).where(Foo["id"] == new_foo.id))
            .scalars()
            .one_or_none()
        )
        assert after is None


def test_create_partial_models():
    partial = Foo.Create(name="Partial Foo")
    assert getattr(partial, "name") == "Partial Foo"
    assert hasattr(partial, "id") is False

    foo = Foo.shell(partial)
    assert foo.name == "Partial Foo"
    assert foo.id is None

    # text ignored fields get defaulted
    foo_with_extra = Foo.shell(Foo.Create(id=2, name="Another"))
    assert foo_with_extra.id is None
    assert foo_with_extra.name == "Another"


def test_update_partial_models():
    partial = Foo.Update(name="Updated Name")

    foo = Foo(id=2, name="Initial Name").absorb(partial)
    assert foo.name == "Updated Name"
    assert foo.id == 2

    with pytest.raises(ValidationError):
        Foo.Update(id=3)  # id is frozen
