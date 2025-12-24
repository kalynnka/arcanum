from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase): ...


class Foo(Base):
    __tablename__ = "foo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)

    bars: Mapped[list[Bar]] = relationship(
        back_populates="foo", uselist=True, cascade="all, delete-orphan"
    )


class Bar(Base):
    __tablename__ = "bar"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data: Mapped[str] = mapped_column(String(50), nullable=False)
    foo_id: Mapped[int] = mapped_column(
        ForeignKey(Foo.id, ondelete="CASCADE"), unique=True
    )
    foo: Mapped[Foo] = relationship(back_populates="bars", uselist=False)


class Car(Base):
    __tablename__ = "car"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model: Mapped[str] = mapped_column(String(50), nullable=False)


__all__ = [
    "Base",
    "Foo",
    "Bar",
]
