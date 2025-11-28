from sqlalchemy.orm import Session

from tests.models import Foo as FooModel
from tests.schemas import Bar as BarProtocol
from tests.schemas import Foo as FooProtocol


def test_bless_foo_into_protocol(foo_with_bar: FooModel, test_session: Session):
    foo_id = foo_with_bar.id
    loaded = test_session.get(FooModel, foo_id)

    assert loaded is not None

    proto = FooProtocol.model_validate(loaded)
    assert proto.id == loaded.id
    assert proto.name == loaded.name
    assert proto.__provided__ is loaded

    assert isinstance(proto.bar.value, BarProtocol)
    assert isinstance(proto.bar.value.foo.value, FooProtocol)

    new_name = "Renamed Foo"
    proto.name = new_name
    assert loaded.name == new_name
    assert proto.name == new_name

    from sqlalchemy.sql import select

    stmt = select(FooModel).where(FooModel.id == foo_id)
    pass
