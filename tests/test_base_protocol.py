import pytest
from pydantic import ValidationError
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from arcanum.database import Session as ArcanumSession
from arcanum.dml import delete, insert, update
from arcanum.selectable import select
from tests.models import Bar as BarModel
from tests.models import Foo as FooModel
from tests.schemas import Bar as BarProtocol
from tests.schemas import Foo as FooProtocol


def test_bless_foo_into_protocol(foo_with_bar: FooModel, engine: Engine):
    foo_id = foo_with_bar.id
    with Session(engine) as session:
        loaded = session.get(FooModel, foo_id)

        assert loaded is not None

        foo = FooProtocol.model_validate(loaded)
        assert foo.id == loaded.id
        assert foo.name == loaded.name
        assert foo.__provided__ is loaded

        assert len(foo.bars) == 2
        assert isinstance(foo.bars[0], BarProtocol)
        assert isinstance(foo.bars[0].foo.value, FooProtocol)
        assert foo.bars[0].foo.value is foo

        foo.bars[0].data = "Updated Bar Data"
        session.flush()

        bar = session.get(BarModel, foo.bars[0].id)
        assert bar is not None
        assert bar.data == foo.bars[0].data


def test_column_expression(engine: Engine, foo_with_bar: FooModel):
    stmt = select(FooProtocol).where(FooProtocol["id"] == foo_with_bar.id)
    with ArcanumSession(engine) as session:
        result = session.execute(stmt)
        foo = result.scalars().one()
        assert foo is not None
        assert foo.id == foo_with_bar.id
        assert foo.name == foo_with_bar.name


def test_adapted_select(engine: Engine, foo_with_bar: FooModel):
    with ArcanumSession(engine) as session:
        stmt = select(FooProtocol, FooModel.name).where(FooModel.id == foo_with_bar.id)
        result = session.execute(stmt)
        row = result.fetchone()
        assert row
        assert isinstance(row[0], FooProtocol)
        assert row[0].id == foo_with_bar.id
        assert row[0].name == foo_with_bar.name
        assert row[1] == foo_with_bar.name


def test_adapted_insert(engine: Engine):
    with ArcanumSession(engine) as session:
        # no returning
        stmt = insert(FooProtocol).values(name="Inserted Foo")
        session.execute(stmt)

        inserted_foo = (
            session.execute(select(FooProtocol).where(FooModel.name == "Inserted Foo"))
            .scalars()
            .one()
        )

        assert inserted_foo is not None
        assert inserted_foo.name == "Inserted Foo"

        # with returning
        stmt = (
            insert(FooProtocol)
            .values(name="Inserted Foo With Returning")
            .returning(FooProtocol)
        )
        result = session.execute(stmt)
        inserted_foo = result.scalars().one()
        assert result


def test_adapted_update(engine: Engine, foo_with_bar: FooModel):
    with ArcanumSession(engine) as session:
        foo_id = foo_with_bar.id

        # update without returning
        stmt = (
            update(FooProtocol).where(FooModel.id == foo_id).values(name="Updated Foo")
        )
        session.execute(stmt)

        updated = session.get(FooModel, foo_id)
        assert updated
        assert updated.name == "Updated Foo"

        # update with returning
        stmt = (
            update(FooProtocol)
            .where(FooModel.id == foo_id)
            .values(name="Updated Foo With Returning")
            .returning(FooProtocol)
        )
        result = session.execute(stmt)
        updated_foo = result.scalars().one()

        assert isinstance(updated_foo, FooProtocol)
        assert updated_foo.id == foo_id
        assert updated_foo.name == "Updated Foo With Returning"


def test_adapted_delete(engine: Engine, foo_with_bar: FooModel):
    with ArcanumSession(engine) as session:
        # Create another foo to delete
        new_foo = FooModel(name="To Be Deleted")
        session.add(new_foo)
        session.flush()
        foo_id = new_foo.id

        # delete with returning
        stmt = delete(FooProtocol).where(FooModel.id == foo_id).returning(FooProtocol)
        result = session.execute(stmt)
        deleted_foo = result.scalars().one()

        assert isinstance(deleted_foo, FooProtocol)
        assert deleted_foo.id == foo_id
        assert deleted_foo.name == "To Be Deleted"

        # verify it's actually deleted
        after = (
            session.execute(select(FooProtocol).where(FooModel.id == foo_id))
            .scalars()
            .one_or_none()
        )
        assert after is None


def test_create_partial_models():
    partial = FooProtocol.Create(name="Partial Foo")
    assert getattr(partial, "name") == "Partial Foo"
    assert hasattr(partial, "id") is False

    foo = FooProtocol.shell(partial)
    assert foo.name == "Partial Foo"
    assert foo.id is None

    # text ignored fields get defaulted
    foo_with_extra = FooProtocol.shell(FooProtocol.Create(id=2, name="Another"))
    assert foo_with_extra.id is None
    assert foo_with_extra.name == "Another"


def test_update_partial_models():
    partial = FooProtocol.Update(name="Updated Name")

    foo = FooProtocol(id=2, name="Initial Name").absorb(partial)
    assert foo.name == "Updated Name"
    assert foo.id == 2

    with pytest.raises(ValidationError):
        FooProtocol.Update(id=3)  # id is frozen
