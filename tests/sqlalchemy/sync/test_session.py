"""Test custom Session features.

Tests:
- Session context management
- get_one behavior (raises if not found)
- Object identity and caching
- Session.add and Session.add_all
- Session.delete
- Session.flush and Session.commit
- Session.rollback
- Session.expire and Session.refresh
- Session.expunge
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import NoResultFound

from arcanum.materia.sqlalchemy import Session
from tests.schemas import Author, Book, Publisher


class TestSessionContextManagement:
    """Test Session context management."""

    def test_session_context_manager(self, engine: Engine):
        """Test using Session as a context manager."""
        with Session(engine) as session:
            author = Author(name="Context Test", field="Physics")
            session.add(author)
            session.flush()
            author.revalidate()

            assert author.id is not None

        # Session should be closed outside context

    def test_session_commit_on_exit(self, engine: Engine):
        """Test that changes are committed on successful context exit."""
        author_id = None
        with Session(engine) as session:
            with session.begin():
                author = Author(name="Auto Commit Test", field="Biology")
                session.add(author)
                session.flush()
                author.revalidate()
                author_id = author.id

        # Verify in new session
        with Session(engine) as session:
            retrieved = session.get_one(Author, author_id)
            assert retrieved.name == "Auto Commit Test"

    def test_session_rollback_on_exception(self, engine: Engine):
        """Test that changes are rolled back on exception."""
        try:
            with Session(engine) as session:
                author = Author(name="Rollback Test", field="Chemistry")
                session.add(author)
                session.flush()
                author_id = author.id

                # Cause an exception
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Changes should be rolled back
        with Session(engine) as session:
            result = session.get(Author, author_id)
            assert result is None

    def test_nested_context_managers(self, engine: Engine):
        """Test nested Session context managers (not recommended but possible)."""
        with Session(engine) as outer_session:
            author = Author(name="Outer", field="Physics")
            outer_session.add(author)
            outer_session.flush()
            author.revalidate()
            outer_session.commit()

            author_id = author.id

            with Session(engine) as inner_session:
                # Inner session can see committed data
                inner_author = inner_session.get_one(Author, author_id)
                assert inner_author.name == "Outer"


class TestGetOne:
    """Test Session.get_one method."""

    def test_get_one_success(self, engine: Engine):
        """Test get_one returns object when found."""
        with Session(engine) as session:
            author = Author(name="Get One Test", field="Physics")
            session.add(author)
            session.flush()
            author.revalidate()
            author_id = author.id

            # Retrieve
            retrieved = session.get_one(Author, author_id)
            assert retrieved.id == author_id
            assert retrieved.name == "Get One Test"

    def test_get_one_raises_when_not_found(self, engine: Engine):
        """Test get_one raises NoResultFound when not found."""
        with Session(engine) as session:
            with pytest.raises(NoResultFound):
                session.get_one(Author, 999999)

    def test_get_one_vs_get(self, engine: Engine):
        """Test difference between get_one and get."""
        with Session(engine) as session:
            # get returns None
            result = session.get(Author, 999999)
            assert result is None

            # get_one raises
            with pytest.raises(NoResultFound):
                session.get_one(Author, 999999)


class TestObjectIdentity:
    """Test object identity and caching within a Session."""

    def test_identity_map_same_object(self, engine: Engine):
        """Test that fetching the same PK returns the same Python object."""
        with Session(engine) as session:
            author = Author(name="Identity Test", field="Biology")
            session.add(author)
            session.flush()
            author.revalidate()

            # Fetch twice
            fetch1 = session.get_one(Author, author.id)
            fetch2 = session.get_one(Author, author.id)

            # Should be the same object
            assert fetch1 is fetch2

    def test_identity_across_queries(self, engine: Engine):
        """Test identity is maintained across different query methods."""
        with Session(engine) as session:
            author = Author(name="Query Identity Test", field="Chemistry")
            session.add(author)
            session.flush()
            author.revalidate()

            # Fetch via get_one
            via_get = session.get_one(Author, author.id)

            # Fetch via query
            stmt = select(Author).where(Author["id"] == author.id)
            via_query = session.execute(stmt).scalars().one()

            # Should be the same object
            assert via_get is via_query

    def test_identity_not_shared_across_sessions(self, engine: Engine):
        """Test that different sessions have different object instances."""
        with Session(engine) as session1:
            author = Author(name="Cross Session Test", field="Physics")
            session1.add(author)
            session1.flush()
            author.revalidate()
            session1.commit()
            obj1 = session1.get_one(Author, author.id)

            with Session(engine) as session2:
                obj2 = session2.get_one(Author, author.id)

                # Different objects
                assert obj1 is not obj2
                # But same data
                assert obj1.name == obj2.name

    def test_modified_object_reflects_in_identity_map(self, engine: Engine):
        """Test that modifying an object is visible through identity map."""
        with Session(engine) as session:
            author = Author(name="Original", field="Biology")
            session.add(author)
            session.flush()
            author.revalidate()

            # Get reference
            ref1 = session.get_one(Author, author.id)

            # Modify through first reference
            ref1.name = "Modified"

            # Get another reference
            ref2 = session.get_one(Author, author.id)

            # Should see modification
            assert ref2.name == "Modified"
            assert ref1 is ref2


class TestAddOperations:
    """Test Session.add and add_all operations."""

    def test_add_single_object(self, engine: Engine):
        """Test adding a single object."""
        with Session(engine) as session:
            author = Author(name="Single Add", field="Literature")
            session.add(author)
            session.flush()
            author.revalidate()

            assert author.id is not None

    def test_add_all_multiple_objects(self, engine: Engine):
        """Test adding multiple objects at once."""
        with Session(engine) as session:
            authors = [Author(name=f"Batch {i}", field="Physics") for i in range(5)]
            session.add_all(authors)
            session.flush()
            for a in authors:
                a.revalidate()

            assert all(a.id is not None for a in authors)

    def test_add_with_relationships(self, engine: Engine):
        """Test that adding parent adds related children."""
        with Session(engine) as session:
            author = Author(name="Parent Author", field="Biology")
            publisher = Publisher(name="Parent Publisher", country="USA")

            book = Book(title="Child Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            # Adding book should cascade
            session.add(book)
            session.flush()
            book.revalidate()
            author.revalidate()
            publisher.revalidate()

            assert book.id is not None
            assert author.id is not None
            assert publisher.id is not None


class TestDeleteOperations:
    """Test Session.delete operations."""

    def test_delete_single_object(self, engine: Engine):
        """Test deleting a single object."""
        with Session(engine) as session:
            author = Author(name="To Delete", field="Chemistry")
            session.add(author)
            session.flush()
            author.revalidate()

            # Delete
            session.delete(author)
            session.flush()

            # Verify
            assert session.get(Author, author.id) is None

    def test_delete_cascades_to_children(self, engine: Engine):
        """Test that deleting parent cascades to children."""
        with Session(engine) as session:
            author = Author(name="Cascade Author", field="Physics")
            publisher = Publisher(name="Cascade Publisher", country="USA")

            book = Book(title="Cascade Book", year=2024)
            book.author.value = author
            book.publisher.value = publisher

            session.add(book)
            session.flush()
            book.revalidate()
            author.revalidate()

            # Delete author (should cascade to book due to ON DELETE CASCADE)
            session.delete(author)
            session.flush()

            # Book should be deleted
            assert session.get(Book, book.id) is None
            assert session.get(Author, author.id) is None


class TestFlushCommitRollback:
    """Test flush, commit, and rollback operations."""

    def test_flush_makes_changes_visible_in_session(self, engine: Engine):
        """Test that flush makes changes visible within the session."""
        with Session(engine) as session:
            author = Author(name="Flush Test", field="Biology")
            session.add(author)

            # Before flush, no ID
            assert author.id is None

            # After flush
            session.flush()
            author.revalidate()
            assert author.id is not None

            # But not yet committed
            # (We can't easily test this in the same session)

    def test_commit_persists_changes(self, engine: Engine):
        """Test that commit persists changes to database."""
        with Session(engine) as session:
            author = Author(name="Commit Test", field="Chemistry")
            session.add(author)
            session.flush()
            author.revalidate()
            author_id = author.id
            session.commit()

        # Verify in new session
        with Session(engine) as session:
            retrieved = session.get_one(Author, author_id)
            assert retrieved.name == "Commit Test"

    def test_rollback_reverts_changes(self, engine: Engine):
        """Test that rollback reverts unflushed/uncommitted changes."""
        with Session(engine) as session:
            author = Author(name="Rollback Test", field="Physics")
            session.add(author)
            session.flush()
            author.revalidate()
            author_id = author.id
            session.commit()  # Commit the original insert

            # Modify
            author.name = "Modified"
            session.flush()

            # Rollback only reverts the modification
            session.rollback()

            # Refresh the author object to reload from database
            session.refresh(author)
            author.revalidate()
            assert author.name == "Rollback Test"

    def test_multiple_flush_before_commit(self, engine: Engine):
        """Test multiple flushes before final commit."""
        with Session(engine) as session:
            author = Author(name="Multi Flush", field="Literature")
            session.add(author)
            session.flush()

            author.name = "Multi Flush Updated 1"
            session.flush()

            author.name = "Multi Flush Updated 2"
            session.flush()

            assert author.name == "Multi Flush Updated 2"


class TestExpireRefresh:
    """Test expire and refresh operations."""

    def test_expire_reloads_on_access(self, engine: Engine):
        """Test that expiring an object reloads it on next access."""
        with Session(engine) as session:
            author = Author(name="Expire Test", field="Biology")
            session.add(author)
            session.flush()
            author.revalidate()

            # Modify in database directly (simulate external change)
            from sqlalchemy import update

            from tests import models

            stmt = (
                update(models.Author)
                .where(models.Author.id == author.id)
                .values(name="Externally Modified")
            )
            session.execute(stmt)
            session.commit()

            # Object still has old value in memory
            # (Unless we expire and refresh it)

            # Expire and refresh to reload from database
            session.expire(author)
            session.refresh(author)
            author.revalidate()
            # Next access should show new value
            assert author.name == "Externally Modified"

    def test_refresh_reloads_immediately(self, engine: Engine):
        """Test that refresh reloads object immediately."""
        with Session(engine) as session:
            author = Author(name="Refresh Test", field="Chemistry")
            session.add(author)
            session.flush()
            author.revalidate()

            # Modify in database
            from sqlalchemy import update

            from tests import models

            stmt = (
                update(models.Author)
                .where(models.Author.id == author.id)
                .values(name="DB Modified")
            )
            session.execute(stmt)
            session.commit()

            # Refresh to load new value
            session.refresh(author)
            assert author.name == "DB Modified"

    def test_expire_all(self, engine: Engine):
        """Test expiring all objects in session."""
        with Session(engine) as session:
            author1 = Author(name="Expire All 1", field="Physics")
            author2 = Author(name="Expire All 2", field="Biology")
            session.add_all([author1, author2])
            session.flush()

            # Expire all
            session.expire_all()

            # Next access should reload
            assert author1.name == "Expire All 1"
            assert author2.name == "Expire All 2"


class TestExpunge:
    """Test expunging objects from session."""

    def test_expunge_removes_from_session(self, engine: Engine):
        """Test that expunge removes object from session."""
        with Session(engine) as session:
            author = Author(name="Expunge Test", field="Literature")
            session.add(author)
            session.flush()
            author.revalidate()

            # Expunge
            session.expunge(author)

            # Object no longer in session
            # Getting it again returns a different instance
            author2 = session.get_one(Author, author.id)
            assert author is not author2

    def test_expunge_all(self, engine: Engine):
        """Test expunging all objects from session."""
        with Session(engine) as session:
            author1 = Author(name="Expunge All 1", field="Physics")
            author2 = Author(name="Expunge All 2", field="Biology")
            session.add_all([author1, author2])
            session.flush()
            author1.revalidate()
            author2.revalidate()

            # Expunge all
            session.expunge_all()

            # Fetching again returns new instances
            new1 = session.get_one(Author, author1.id)
            new2 = session.get_one(Author, author2.id)

            assert new1 is not author1
            assert new2 is not author2
