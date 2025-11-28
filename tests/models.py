from __future__ import annotations

from typing import Optional

from sqlalchemy import ForeignKey, Integer, MetaData, String
from sqlalchemy.orm import Mapped, mapped_column, registry, relationship

metadata = MetaData()
mapper_registry = registry(metadata=metadata)


@mapper_registry.as_declarative_base()
class Base: ...


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


class Car(Base):
    __tablename__ = "car"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model: Mapped[str] = mapped_column(String(50), nullable=False)


__all__ = [
    "Base",
    "Foo",
    "Bar",
]
