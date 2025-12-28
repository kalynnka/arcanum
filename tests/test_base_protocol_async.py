import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from arcanum.database import AsyncSession
from tests.models import Foo as FooModel
from tests.schemas import Bar, Foo


@pytest.mark.asyncio
async def test_bless_foo_into_protocol_async(
    async_engine: AsyncEngine,
    async_foo_with_bar: Foo,
):
    async with AsyncSession(async_engine) as session:
        foo = await session.get_one(Foo, async_foo_with_bar.id)

        assert foo.id == async_foo_with_bar.id
        assert foo.name == async_foo_with_bar.name
        assert isinstance(foo.__provided__, FooModel)

        # explicitly trigger the loading of relationships
        await foo.bars

        assert len(foo.bars) == 2
        assert isinstance(foo.bars[0], Bar)
        assert isinstance(foo.bars[0].foo.value, Foo)
        assert foo.bars[0].foo.value is foo

        foo.bars[0].data = "Updated Bar Data"
        await session.flush()

        bar = await session.get(Bar, foo.bars[0].id)
        assert bar is not None
        assert bar.data == foo.bars[0].data


@pytest.mark.asyncio
async def test_column_expression_async(
    async_engine: AsyncEngine, async_foo_with_bar: Foo
):
    stmt = select(Foo).where(Foo["id"] == async_foo_with_bar.id)
    async with AsyncSession(async_engine) as session:
        result = await session.execute(stmt)
        foo = result.scalars().one()
        assert foo is not None
        assert foo.id == async_foo_with_bar.id
        assert foo.name == async_foo_with_bar.name


@pytest.mark.asyncio
async def test_adapted_select_async(async_engine: AsyncEngine, async_foo_with_bar: Foo):
    async with AsyncSession(async_engine) as session:
        stmt = select(Foo, Foo["name"]).where(Foo["id"] == async_foo_with_bar.id)
        result = await session.execute(stmt)
        row = result.fetchone()
        assert row
        assert isinstance(row[0], Foo)
        assert row[0].id == async_foo_with_bar.id
        assert row[0].name == async_foo_with_bar.name
        assert row[1] == async_foo_with_bar.name


@pytest.mark.asyncio
async def test_adapted_insert_async(async_engine: AsyncEngine):
    async with AsyncSession(async_engine) as session:
        # no returning
        stmt = insert(Foo).values(name="Inserted Foo")
        await session.execute(stmt)

        inserted_foo = (
            (await session.execute(select(Foo).where(Foo["name"] == "Inserted Foo")))
            .scalars()
            .one()
        )

        assert inserted_foo is not None
        assert inserted_foo.name == "Inserted Foo"

        # with returning
        stmt = (
            insert(Foo)
            .values(name="Inserted Foo With Returning")
            .returning(Foo, Foo["id"])
        )
        result = await session.execute(stmt)
        inserted_foo = result.scalars().one()
        assert result


@pytest.mark.asyncio
async def test_adapted_update_async(async_engine: AsyncEngine, async_foo_with_bar: Foo):
    async with AsyncSession(async_engine) as session:
        foo_id = async_foo_with_bar.id

        # update without returning
        stmt = update(Foo).where(Foo["id"] == foo_id).values(name="Updated Foo")
        await session.execute(stmt)

        updated1 = await session.get(Foo, foo_id)
        assert updated1
        assert updated1.name == "Updated Foo"

        # update with returning
        stmt = (
            update(Foo)
            .where(Foo["id"] == foo_id)
            .values(name="Updated Foo With Returning")
            .returning(Foo)
        )
        result = await session.execute(stmt)
        updated2 = result.scalars().one()

        # TODO: We need an manual refresh/revalidate operation here to keep the transmuter object sync with the orm
        # as the same foo orm object is loaded earlier within the same session and kept in session's identity map,
        # Sqlalchemy's synchronization will update the orm state with an update/insert statement
        # while we lack of a mecanisum (or not that proper) to notify the transmuter object to revalidate automatically
        # see https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#selecting-a-synchronization-strategy
        # currently the candidates would be
        # 1. the 'refresh' mapper event, but not that ideal for it is registered on mapper class not instance.
        # 2. try to trigger the revalidate on update/insert statement execution if the statement execution option have synchronize_session is not False
        updated2.revalidate()

        assert isinstance(updated2, Foo)
        assert updated1 is updated2
        assert updated1.__provided__ is updated2.__provided__
        assert updated1.name == updated2.name == "Updated Foo With Returning"

        # example of synchronize_session=False
        stmt = (
            update(Foo)
            .where(Foo["id"] == foo_id)
            .values(name="Updated Foo With Returning Again")
            .execution_options(synchronize_session=False)
            .returning(Foo)
        )
        result = await session.execute(stmt)
        updated3 = result.scalars().one()

        assert updated1 is updated2 is updated3
        assert updated1.__provided__ is updated2.__provided__ is updated3.__provided__
        assert (
            updated1.__provided__.name
            == updated2.__provided__.name
            == updated3.__provided__.name
            == "Updated Foo With Returning"
        )

        await session.refresh(updated3)

        assert updated1 is updated2 is updated3
        assert updated1.__provided__ is updated2.__provided__ is updated3.__provided__
        assert (
            updated1.__provided__.name
            == updated2.__provided__.name
            == updated3.__provided__.name
            == "Updated Foo With Returning Again"
        )


@pytest.mark.asyncio
async def test_adapted_delete_async(async_engine: AsyncEngine):
    async with AsyncSession(async_engine) as session:
        # Create another foo to delete
        new_foo = Foo(name="To Be Deleted")
        session.add(new_foo)
        await session.flush()
        # TODO: we must revalidate here to sync the returned identity keys from orm to transmuter object
        # revalidate won't issue an additional select statement as the identity keys are be already returned by sqlalchemy (with dialects that support returning)
        # except for Mysql :xd, in that case do session.refresh(new_foo)
        new_foo.revalidate()

        # delete with returning
        stmt = delete(Foo).where(Foo["id"] == new_foo.id).returning(Foo)
        result = await session.execute(stmt)
        deleted_foo = result.scalars().one()

        assert isinstance(deleted_foo, Foo)
        assert deleted_foo.id == new_foo.id
        assert deleted_foo.name == "To Be Deleted"

        # verify it's actually deleted
        after = (
            (await session.execute(select(Foo).where(Foo["id"] == new_foo.id)))
            .scalars()
            .one_or_none()
        )
        assert after is None
