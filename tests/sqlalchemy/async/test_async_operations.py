"""Test async SQLAlchemy materia support.

Tests:
- AsyncSession context management
- AsyncResult streaming and iteration
- Basic async CRUD operations
- Async relationship loading

Note: These tests are less detailed than sync tests because SQLAlchemy
reuses sync code via greenlet, so comprehensive testing is done in sync tests.
"""

from __future__ import annotations

from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import selectinload

from arcanum.materia.sqlalchemy import AsyncSession
from tests import models
from tests.schemas import Author, Book, Publisher


class TestAsyncSessionContextManagement:
    """Test AsyncSession context manager behavior."""

    @pytest.mark.asyncio
    async def test_async_session_auto_commit_on_success(
        self, async_engine: AsyncEngine, test_id: UUID
    ):
        """Test that async session auto-commits on successful exit."""
        async with AsyncSession(async_engine) as session:
            async with session.begin():
                author = Author(name="Async Author", field="Physics", test_id=test_id)
                session.add(author)
                await session.flush()
                author.revalidate()

        async with AsyncSession(async_engine) as session:
            fetched = await session.get_one(Author, author.id)
            assert fetched.name == "Async Author"

    @pytest.mark.asyncio
    async def test_async_session_rollback_on_exception(self, async_engine: AsyncEngine):
        """Test that async session rolls back on exception."""
        try:
            async with AsyncSession(async_engine) as session:
                author = Author(name="Rollback Test", field="Biology")
                session.add(author)
                await session.flush()

                # Force exception
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Author should not be committed
        async with AsyncSession(async_engine) as session:
            stmt = select(Author).where(Author["name"] == "Rollback Test")
            result = await session.execute(stmt)
            authors = result.scalars().all()
            assert len(authors) == 0


class TestAsyncCRUDOperations:
    """Test basic async CRUD operations."""

    @pytest.mark.asyncio
    async def test_async_create_and_read(self, async_engine: AsyncEngine):
        """Test async insert and select."""
        async with AsyncSession(async_engine) as session:
            author = Author(name="Insert Author", field="Chemistry")
            session.add(author)
            await session.flush()

            # Select
            author.revalidate()
            fetched = await session.get_one(Author, author.id)
            assert fetched.name == "Insert Author"

    @pytest.mark.asyncio
    async def test_async_update(self, async_engine: AsyncEngine):
        """Test async update operation."""
        async with AsyncSession(async_engine) as session:
            author = Author(name="Update Test", field="Physics")
            session.add(author)
            await session.flush()

            # Update
            author.name = "Updated Async"
            await session.flush()

            # Verify
            author.revalidate()
            fetched = await session.get_one(Author, author.id)
            assert fetched.name == "Updated Async"

    @pytest.mark.asyncio
    async def test_async_delete(self, async_engine: AsyncEngine):
        """Test async delete operation."""
        async with AsyncSession(async_engine) as session:
            author = Author(name="Delete Test", field="Biology")
            session.add(author)
            await session.flush()

            # Delete
            author.revalidate()
            await session.delete(author)
            await session.flush()

            # Verify deleted
            stmt = select(Author).where(Author["id"] == author.id)
            result = await session.execute(stmt)
            authors = result.scalars().all()
            assert len(authors) == 0


class TestAsyncResultStreaming:
    """Test AsyncResult streaming capabilities."""

    @pytest.mark.asyncio
    async def test_async_stream_scalars(self, async_engine: AsyncEngine):
        """Test async streaming scalars."""
        # Create test data
        async with AsyncSession(async_engine) as session:
            authors = [
                Author(name=f"Stream Author {i}", field="Physics") for i in range(5)
            ]
            session.add_all(authors)
            await session.flush()

            stmt = select(Author).where(Author["name"].startswith("Stream Author"))
            result = await session.stream(stmt)

            count = 0
            async for author in result.scalars():
                assert author.name.startswith("Stream Author")
                count += 1

            assert count == 5

    @pytest.mark.asyncio
    async def test_async_stream_partitions(self, async_engine: AsyncEngine):
        """Test async streaming with partitions."""
        # Create test data
        async with AsyncSession(async_engine) as session:
            authors = [
                Author(name=f"Partition Author {i}", field="Biology") for i in range(10)
            ]
            session.add_all(authors)
            await session.flush()

            stmt = select(Author).where(Author["name"].startswith("Partition Author"))
            result = await session.stream(stmt)

            partition_count = 0
            async for partition in result.scalars().partitions(3):
                assert len(partition) <= 3
                partition_count += 1

            # Should have 4 partitions (3 + 3 + 3 + 1)
            assert partition_count == 4

    @pytest.mark.asyncio
    async def test_async_stream_unique(self, async_engine: AsyncEngine):
        """Test async streaming with unique() to deduplicate joined results."""
        async with AsyncSession(async_engine) as session:
            # Create author and publisher with books
            author = Author(name="Unique Test", field="Chemistry")
            publisher = Publisher(name="Test Publisher", country="USA")
            book1 = Book(title="Book 1", year=2023)
            book2 = Book(title="Book 2", year=2024)
            book1.author.value = author
            book2.author.value = author
            book1.publisher.value = publisher
            book2.publisher.value = publisher

            session.add(author)
            session.add(publisher)
            await session.flush()

            # Join query - without unique() would return author twice
            stmt = (
                select(Author)
                .join(Book)
                .where(Author["name"] == "Unique Test")
                .options(selectinload(models.Author.books))
            )

            result = await session.stream(stmt)
            authors = []
            async for author in result.scalars().unique():
                authors.append(author)

            # Should get only one author despite two books
            assert len(authors) == 1
            assert len(authors[0].books) == 2


class TestAsyncRelationships:
    """Test async relationship loading."""

    @pytest.mark.asyncio
    async def test_async_eager_loading_selectinload(self, async_engine: AsyncEngine):
        """Test async eager loading with selectinload."""
        async with AsyncSession(async_engine) as session:
            # Create author and publisher with books
            author = Author(name="Eager Author", field="Literature")
            publisher = Publisher(name="Eager Publisher", country="USA")
            books = [Book(title=f"Eager Book {i}", year=2020 + i) for i in range(3)]
            for book in books:
                book.author.value = author
                book.publisher.value = publisher

            session.add(author)
            session.add(publisher)
            await session.flush()

            author.revalidate()

            session.expunge_all()

            stmt = (
                select(Author)
                .where(Author["id"] == author.id)
                .options(selectinload(models.Author.books))
            )
            result = await session.execute(stmt)
            fetched = result.scalars().one()

            # Books should be loaded without additional query

            # Verify no additional SQL is issued when accessing books
            with patch.object(
                session.sync_session, "execute", wraps=session.sync_session.execute
            ) as mock_execute:
                # Access books - should not trigger additional SQL
                assert len(fetched.books) == 3
                # No execute calls should have been made
                assert mock_execute.call_count == 0

    @pytest.mark.asyncio
    async def test_async_many_to_many_relationship(self, async_engine: AsyncEngine):
        """Test async many-to-many relationship access."""
        from tests.schemas import Category

        async with AsyncSession(async_engine) as session:
            # Create book with categories
            author = Author(name="M2M Author", field="Physics")
            publisher = Publisher(name="M2M Publisher", country="UK")
            book = Book(title="M2M Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            cat1 = Category(name="Async Cat 1")
            cat2 = Category(name="Async Cat 2")
            book.categories.extend([cat1, cat2])

            session.add(book)
            await session.flush()

            book.revalidate()
            session.expunge_all()

            stmt = (
                select(Book)
                .where(Book["id"] == book.id)
                .options(selectinload(models.Book.categories))
            )
            result = await session.execute(stmt)
            fetched = result.scalars().one()

            # Categories should be loaded
            assert len(fetched.categories) == 2
            cat_names = {cat.name for cat in fetched.categories}
            assert cat_names == {"Async Cat 1", "Async Cat 2"}

            # Verify no additional SQL is issued when accessing categories
            with patch.object(
                session.sync_session, "execute", wraps=session.sync_session.execute
            ) as mock_execute:
                # Access categories - should not trigger additional SQL
                _ = [cat.name for cat in fetched.categories]
                # No execute calls should have been made
                assert mock_execute.call_count == 0


class TestAsyncComplexQueries:
    """Test async complex query patterns."""

    @pytest.mark.asyncio
    async def test_async_join_with_filter(self, async_engine: AsyncEngine):
        """Test async join with filtering."""
        async with AsyncSession(async_engine) as session:
            # Create test data
            author1 = Author(name="Join Author 1", field="Physics")
            author2 = Author(name="Join Author 2", field="Biology")
            publisher = Publisher(name="Join Publisher", country="USA")

            book1 = Book(title="Join Book 1", year=2020)
            book2 = Book(title="Join Book 2", year=2023)
            book1.author.value = author1
            book2.author.value = author2
            book1.publisher.value = publisher
            book2.publisher.value = publisher

            session.add_all([book1, book2])
            await session.flush()

            # Join query with filter
            stmt = select(Book).join(Author).where(Author["field"] == "Physics")
            result = await session.execute(stmt)
            books = result.scalars().all()

            assert len(books) == 1
            assert books[0].title == "Join Book 1"

    @pytest.mark.asyncio
    async def test_async_aggregate_query(self, async_engine: AsyncEngine):
        """Test async aggregate query."""
        from sqlalchemy import func

        async with AsyncSession(async_engine) as session:
            # Create test data
            author = Author(name="Agg Author", field="History")
            publisher = Publisher(name="Agg Publisher", country="UK")
            books = [Book(title=f"Agg Book {i}", year=2020 + i) for i in range(5)]
            for book in books:
                book.author.value = author
                book.publisher.value = publisher

            session.add(author)
            session.add(publisher)
            await session.flush()

            # Aggregate query
            author.revalidate()
            stmt = (
                select(Author, func.count(Book["id"]))
                .join(Book)
                .where(Author["id"] == author.id)
                .group_by(Author["id"])
            )
            result = await session.execute(stmt)
            row = result.one()

            assert row[1] == 5  # Book count

    @pytest.mark.asyncio
    async def test_async_subquery(self, async_engine: AsyncEngine):
        """Test async query with subquery."""
        from sqlalchemy import func

        async with AsyncSession(async_engine) as session:
            # Create test data
            publisher = Publisher(name="Subquery Pub", country="USA")
            books = [Book(title=f"Subquery Book {i}", year=2020 + i) for i in range(3)]
            author = Author(name="Subquery Author", field="Literature")
            for book in books:
                book.publisher.value = publisher
                book.author.value = author

            session.add(publisher)
            await session.flush()

            # Subquery to get publishers with more than 2 books
            subq = (
                select(Publisher["id"])
                .join(Book)
                .group_by(Publisher["id"])
                .having(func.count(Book["id"]) > 2)
                .scalar_subquery()
            )
            models.Author.id.in_(subq)
            stmt = select(Publisher).where(Publisher["id"].in_(subq))
            result = await session.execute(stmt)
            publishers = result.scalars().all()

            publisher.revalidate()
            assert len(publishers) == 1
            assert publishers[0].id == publisher.id
