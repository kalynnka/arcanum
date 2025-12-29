import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from arcanum.database import AsyncSession
from tests import models
from tests.schemas import Author, Book, BookDetail, Category, Publisher


@pytest.mark.asyncio
async def test_bless_book_into_protocol_async(
    async_engine: AsyncEngine,
    book_with_relations: Book,
):
    async with AsyncSession(async_engine) as session:
        book = await session.get_one(Book, book_with_relations.id)

        assert book.id == book_with_relations.id
        assert book.title == book_with_relations.title
        assert isinstance(
            book.__transmuter_provided__,
            type(book_with_relations.__transmuter_provided__),
        )

        # explicitly trigger the loading of relationships
        await book.author
        await book.publisher
        await book.detail
        await book.categories

        # Test M-1 relationship: Book -> Author
        assert isinstance(book.author.value, Author)
        assert book.author.value.name == "Stephen Hawking"

        # Test M-1 relationship: Book -> Publisher
        assert isinstance(book.publisher.value, Publisher)
        assert book.publisher.value.name == "Bantam Books"

        # Test 1-1 relationship: Book -> BookDetail
        assert isinstance(book.detail.value, BookDetail)
        assert book.detail.value.isbn == "978-0553380163"
        assert book.detail.value.book.value is book

        # Test M-M relationship: Book -> Categories
        assert len(book.categories) == 2
        assert isinstance(book.categories[0], Category)


@pytest.mark.asyncio
async def test_column_expression_async(
    async_engine: AsyncEngine, book_with_relations: Book
):
    stmt = select(Book).where(Book["id"] == book_with_relations.id)
    async with AsyncSession(async_engine) as session:
        result = await session.execute(stmt)
        book = result.scalars().one()
        assert book is not None
        assert book.id == book_with_relations.id
        assert book.title == book_with_relations.title


@pytest.mark.asyncio
async def test_adapted_select_async(
    async_engine: AsyncEngine, book_with_relations: Book
):
    async with AsyncSession(async_engine) as session:
        stmt = select(Book, Book["title"]).where(Book["id"] == book_with_relations.id)
        result = await session.execute(stmt)
        row = result.fetchone()
        assert row
        assert isinstance(row[0], Book)
        assert row[0].id == book_with_relations.id
        assert row[0].title == book_with_relations.title
        assert row[1] == book_with_relations.title


@pytest.mark.asyncio
async def test_adapted_insert_async(async_engine: AsyncEngine):
    async with AsyncSession(async_engine) as session:
        # First create required Author and Publisher
        author = Author(name="Marie Curie", field="Chemistry")
        publisher = Publisher(name="Academic Press", country="France")
        session.add(author)
        session.add(publisher)
        await session.flush()

        author.revalidate()
        publisher.revalidate()

        # no returning
        stmt = insert(Book).values(
            title="Radioactivity",
            year=1910,
            author_id=author.id,
            publisher_id=publisher.id,
        )
        await session.execute(stmt)

        inserted_book = (
            (
                await session.execute(
                    select(Book).where(Book["title"] == "Radioactivity")
                )
            )
            .scalars()
            .one()
        )

        assert inserted_book is not None
        assert inserted_book.title == "Radioactivity"

        # with returning
        stmt = (
            insert(Book)
            .values(
                title="Treatise on Radioactivity",
                year=1914,
                author_id=author.id,
                publisher_id=publisher.id,
            )
            .returning(Book, Book["id"])
        )
        result = await session.execute(stmt)
        inserted_book = result.scalars().one()
        assert result


@pytest.mark.asyncio
async def test_adapted_update_async(
    async_engine: AsyncEngine, book_with_relations: Book
):
    async with AsyncSession(async_engine) as session:
        book_id = book_with_relations.id

        # update without returning
        stmt = update(Book).where(Book["id"] == book_id).values(title="Updated Book")
        await session.execute(stmt)

        updated1 = await session.get(Book, book_id)
        assert updated1
        assert updated1.title == "Updated Book"

        # update with returning
        stmt = (
            update(Book)
            .where(Book["id"] == book_id)
            .values(title="Updated Book With Returning")
            .returning(Book)
        )
        result = await session.execute(stmt)
        updated2 = result.scalars().one()

        # TODO: We need an manual refresh/revalidate operation here to keep the transmuter object sync with the orm
        # as the same book orm object is loaded earlier within the same session and kept in session's identity map,
        # Sqlalchemy's synchronization will update the orm state with an update/insert statement
        # while we lack of a mecanisum (or not that proper) to notify the transmuter object to revalidate automatically
        # see https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#selecting-a-synchronization-strategy
        # currently the candidates would be
        # 1. the 'refresh' mapper event, but not that ideal for it is registered on mapper class not instance.
        # 2. try to trigger the revalidate on update/insert statement execution if the statement execution option have synchronize_session is not False
        updated2.revalidate()

        assert isinstance(updated2, Book)
        assert updated1 is updated2
        assert updated1.__transmuter_provided__ is updated2.__transmuter_provided__
        assert updated1.title == updated2.title == "Updated Book With Returning"

        # example of synchronize_session=False
        stmt = (
            update(Book)
            .where(Book["id"] == book_id)
            .values(title="Updated Book With Returning Again")
            .execution_options(synchronize_session=False)
            .returning(Book)
        )
        result = await session.execute(stmt)
        updated3 = result.scalars().one()

        assert updated1 is updated2 is updated3
        assert (
            updated1.__transmuter_provided__
            is updated2.__transmuter_provided__
            is updated3.__transmuter_provided__
        )
        assert isinstance(updated1.__transmuter_provided__, models.Book)
        assert isinstance(updated2.__transmuter_provided__, models.Book)
        assert isinstance(updated3.__transmuter_provided__, models.Book)
        assert (
            updated1.__transmuter_provided__.title
            == updated2.__transmuter_provided__.title
            == updated3.__transmuter_provided__.title
            == "Updated Book With Returning"
        )

        await session.refresh(updated3)

        assert updated1 is updated2 is updated3
        assert (
            updated1.__transmuter_provided__
            is updated2.__transmuter_provided__
            is updated3.__transmuter_provided__
        )
        assert (
            updated1.__transmuter_provided__.title
            == updated2.__transmuter_provided__.title
            == updated3.__transmuter_provided__.title
            == "Updated Book With Returning Again"
        )


@pytest.mark.asyncio
async def test_adapted_delete_async(async_engine: AsyncEngine):
    async with AsyncSession(async_engine) as session:
        # Create required Author and Publisher
        author = Author(name="George Orwell", field="Dystopian Fiction")
        publisher = Publisher(name="Secker & Warburg", country="United Kingdom")
        session.add(author)
        session.add(publisher)
        await session.flush()
        author.revalidate()
        publisher.revalidate()

        # Create a book to delete
        new_book = Book(
            title="Nineteen Eighty-Four",
            year=1949,
            author_id=author.id,
            publisher_id=publisher.id,
        )
        session.add(new_book)
        await session.flush()
        # TODO: we must revalidate here to sync the returned identity keys from orm to transmuter object
        # revalidate won't issue an additional select statement as the identity keys are be already returned by sqlalchemy (with dialects that support returning)
        # except for Mysql :xd, in that case do session.refresh(new_book)
        new_book.revalidate()

        # delete with returning
        stmt = delete(Book).where(Book["id"] == new_book.id).returning(Book)
        result = await session.execute(stmt)
        deleted_book = result.scalars().one()

        assert isinstance(deleted_book, Book)
        assert deleted_book.id == new_book.id
        assert deleted_book.title == "Nineteen Eighty-Four"
        assert deleted_book.year == 1949
        assert deleted_book.author_id == author.id
        assert deleted_book.publisher_id == publisher.id

        # verify it's actually deleted
        after = (
            (await session.execute(select(Book).where(Book["id"] == new_book.id)))
            .scalars()
            .one_or_none()
        )
        assert after is None
