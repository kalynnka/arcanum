from __future__ import annotations

from typing import Optional

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase): ...


class Foo(Base):
    __tablename__ = "foo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)

    bar: Mapped[Optional[Bar]] = relationship(
        back_populates="foo", uselist=False, cascade="all, delete-orphan"
    )


class Bar(Base):
    __tablename__ = "bar"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data: Mapped[str] = mapped_column(String(50), nullable=False)
    foo_id: Mapped[int] = mapped_column(
        ForeignKey(Foo.id, ondelete="CASCADE"), unique=True
    )
    foo: Mapped[Foo] = relationship(back_populates="bar", uselist=False)


__all__ = [
    "Base",
    "Foo",
    "Bar",
    "metadata_obj",
]
