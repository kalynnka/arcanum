from sqlalchemy import Engine
from sqlalchemy.orm import Session

from arcanum.database import Session as ArcanumSession
from arcanum.selectable import select
from tests.models import Foo as FooModel
from tests.schemas import Bar as BarProtocol
from tests.schemas import Foo as FooProtocol


def test_bless_foo_into_protocol(foo_with_bar: FooModel, engine: Engine):
    foo_id = foo_with_bar.id
    with Session(engine) as session:
        loaded = session.get(FooModel, foo_id)

        assert loaded is not None

        proto = FooProtocol.model_validate(loaded)
        assert proto.id == loaded.id
        assert proto.name == loaded.name
        assert proto.__provided__ is loaded

        assert isinstance(proto.bar.value, BarProtocol)
        assert isinstance(proto.bar.value.foo.value, FooProtocol)
        assert proto.bar.value.foo.value is proto

        new_name = "Renamed Foo"
        proto.name = new_name
        assert loaded.name == new_name
        assert proto.name == new_name


def test_adapted_select(engine: Engine, foo_with_bar: FooModel):
    # stmt = select(FooProtocol)

    from sqlalchemy.sql import select as sa_select

    stmt = sa_select(FooModel.id, FooModel.name)
    # result = test_session.execute(stmt)
    # proto = result.scalars()

    with ArcanumSession(engine) as session:
        stmt = select(FooProtocol, FooModel.name)
        result = session.execute(stmt)
        row = result.fetchone()
        assert row
        assert isinstance(row[0], FooProtocol)
        assert row[0].id == foo_with_bar.id
        assert row[0].name == foo_with_bar.name
        assert row[1] == foo_with_bar.name


def test_adapted_insert(engine: Engine):
    from arcanum.database import Session as ArcanumSession
    from arcanum.dml import AdaptedInsert

    with ArcanumSession(engine) as session:
        # no returning
        stmt = AdaptedInsert(FooProtocol).values(name="Inserted Foo")
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
            AdaptedInsert(FooProtocol)
            .values(name="Inserted Foo With Returning")
            .returning(FooProtocol)
        )
        result = session.execute(stmt)
        inserted_foo = result.scalars().one()
        assert result
