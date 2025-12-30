from __future__ import annotations

from typing import (
    Any,
    Iterable,
    Literal,
    Optional,
    Self,
    Sequence,
    TypeVar,
    Union,
    overload,
)
from weakref import WeakValueDictionary

from sqlalchemy import exc, inspect, tuple_, util
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.interfaces import _CoreAnyExecuteParams, _CoreSingleExecuteParams
from sqlalchemy.engine.result import Result, ScalarResult
from sqlalchemy.ext.asyncio import AsyncSession as SqlalchemyAsyncSession
from sqlalchemy.orm import Query
from sqlalchemy.orm import Session as SqlalchemySession
from sqlalchemy.orm._typing import OrmExecuteOptionsParameter
from sqlalchemy.orm.interfaces import ORMOption
from sqlalchemy.orm.session import (
    JoinTransactionMode,
    _BindArguments,
    _PKIdentityArgument,
    _SessionBind,
    _SessionBindKey,
)
from sqlalchemy.orm.util import Bundle
from sqlalchemy.sql import Executable, Select, functions, select
from sqlalchemy.sql._typing import (
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
    _InfoType,
)
from sqlalchemy.sql.base import ExecutableOption, _NoArg
from sqlalchemy.sql.dml import Delete, Insert, Update, UpdateBase
from sqlalchemy.sql.selectable import ForUpdateArg, ForUpdateParameter, TypedReturnsRows

from arcanum.base import (
    BaseTransmuter,
    ValidateContextGeneratorT,
    ValidationContextT,
    validation_context,
)
from arcanum.expression import Expression
from arcanum.materia.sqlalchemy.result import _T, AdaptedResult
from arcanum.utils import get_cached_adapter

T = TypeVar("T", bound=BaseTransmuter)

ExpressionT = _ColumnExpressionArgument[bool] | Expression[Any]


def resolve_statement_entities(statement: Executable) -> list[type[Any]]:
    entities: list[type[Any]] = []
    if isinstance(statement, Select):
        for desc in statement.column_descriptions:
            if (expr := desc.get("expr")) and (type := desc.get("type")):
                # Bundle types (For example, used by selectinload for pk grouping) return tuple[*]
                if type is Bundle:
                    entities.append(tuple[*(e.type.python_type for e in expr.exprs)])
                else:
                    transmuter = BaseTransmuter.transmuter_formulars.reverse.get(type)
                    entities.append(transmuter or type.python_type)
    elif isinstance(statement, (Insert, Update, Delete)):
        if statement._returning:
            for item in statement._returning:
                if transmuter := BaseTransmuter.transmuter_formulars.reverse.get(
                    item.entity_namespace
                ):
                    entities.append(transmuter)
                else:
                    entities.append(item.type.python_type)  # type: ignore[attr-defined]
    return entities


class Session(SqlalchemySession):
    _validation_context: ValidationContextT
    _validation_context_manager: ValidateContextGeneratorT | None

    def __init__(
        self,
        bind: Optional[_SessionBind] = None,
        *,
        autoflush: bool = True,
        future: Literal[True] = True,
        expire_on_commit: bool = True,
        autobegin: bool = True,
        twophase: bool = False,
        binds: Optional[dict[_SessionBindKey, _SessionBind]] = None,
        enable_baked_queries: bool = True,
        info: Optional[_InfoType] = None,
        query_cls: Optional[type[Query[Any]]] = None,
        autocommit: Literal[False] = False,
        join_transaction_mode: JoinTransactionMode = "conditional_savepoint",
        close_resets_only: Union[bool, _NoArg] = _NoArg.NO_ARG,
    ) -> None:
        super().__init__(
            bind,
            autoflush=autoflush,
            future=future,
            expire_on_commit=expire_on_commit,
            autobegin=autobegin,
            twophase=twophase,
            binds=binds,
            enable_baked_queries=enable_baked_queries,
            info=info,
            query_cls=query_cls,
            autocommit=autocommit,
            join_transaction_mode=join_transaction_mode,
            close_resets_only=close_resets_only,
        )
        self._validation_context = WeakValueDictionary()
        self._validation_context_manager = None

    def __enter__(self):
        self._validation_context_manager = validation_context(self._validation_context)
        self._validation_context_manager.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback) -> Optional[bool]:
        if self._validation_context_manager is not None:
            self._validation_context_manager.__exit__(exc_type, exc_value, traceback)
        return super().__exit__(exc_type, exc_value, traceback)

    def __iter__(self) -> Iterable[BaseTransmuter]:
        if not self._validation_context:
            raise RuntimeError(
                "Active validation context is requried, please use a context manager 'with Session() as session' to create a session context."
            )
        return [self._validation_context[item] for item in super().__iter__()]

    @overload
    def execute(
        self,
        statement: TypedReturnsRows[_T],
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
    ) -> Result[_T]: ...

    @overload
    def execute(
        self,
        statement: UpdateBase,
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
    ) -> CursorResult[Any]: ...
    @overload
    def execute(
        self,
        statement: Executable,
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
    ) -> Result[Any]: ...
    def execute(
        self,
        statement: Executable,
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        _parent_execute_state: Optional[Any] = None,
        _add_event: Optional[Any] = None,
    ) -> Result[Any]:
        result = super().execute(
            statement,
            params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            _parent_execute_state=_parent_execute_state,
            _add_event=_add_event,
        )

        if not execution_options.get("sa_top_level_orm_context", False):
            entities = resolve_statement_entities(statement)
            if entities and any(
                isinstance(e, type) and issubclass(e, BaseTransmuter) for e in entities
            ):
                return AdaptedResult(
                    real_result=result,
                    adapter=get_cached_adapter(tuple[*entities]),
                    scalar_adapter=get_cached_adapter(entities[0]),
                )  # pyright: ignore[reportReturnType]

        return result

    @overload
    def scalar(
        self,
        statement: TypedReturnsRows[tuple[_T]],
        params: Optional[_CoreSingleExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> Optional[_T]: ...
    @overload
    def scalar(
        self,
        statement: Executable,
        params: Optional[_CoreSingleExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> Any: ...
    def scalar(
        self,
        statement: Executable,
        params: Optional[_CoreSingleExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> Any:
        return self.execute(
            statement=statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw,
        ).scalar()

    @overload
    def scalars(
        self,
        statement: TypedReturnsRows[tuple[_T]],
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> ScalarResult[_T]: ...
    @overload
    def scalars(
        self,
        statement: Executable,
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> ScalarResult[Any]: ...
    def scalars(
        self,
        statement: Executable,
        params: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
        **kw: Any,
    ) -> ScalarResult[Any]:
        return self.execute(
            statement=statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw,
        ).scalars()

    def expunge(self, instance: BaseTransmuter) -> None:
        if instance.__transmuter_provided__ in self._validation_context:
            del self._validation_context[instance.__transmuter_provided__]
        super().expunge(instance.__transmuter_provided__)

    def expunge_all(self) -> None:
        self._validation_context.clear()
        return super().expunge_all()

    def refresh(
        self,
        instance: BaseTransmuter,
        attribute_names: Iterable[str] | None = None,
        with_for_update: ForUpdateArg | None | bool | dict[str, Any] = None,
    ) -> None:
        if instance.__transmuter_provided__ not in self._validation_context:
            self._validation_context[instance.__transmuter_provided__] = instance
        super().refresh(instance, attribute_names, with_for_update)
        instance.revalidate()

    def merge(
        self,
        instance: T,
        *,
        load: bool = True,
        options: Sequence[ORMOption] | None = None,
    ) -> T:
        super().merge(instance, load=load, options=options)
        return instance.revalidate()

    def enable_relationship_loading(self, obj: BaseTransmuter) -> None:
        super().enable_relationship_loading(obj.__transmuter_provided__)
        self._validation_context[obj.__transmuter_provided__] = obj

    def get(
        self,
        entity: type[T],
        ident: _PKIdentityArgument,
        *,
        options: Sequence[ORMOption] | None = None,
        populate_existing: bool = False,
        with_for_update: ForUpdateParameter = None,
        identity_token: Optional[Any] = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
    ) -> Optional[T]:
        if isinstance(entity, type) and issubclass(entity, BaseTransmuter):
            instance = super().get(
                # sqlalchemy materia requires transumter to have a provider blessed
                entity.__transmuter_provider__,  # pyright: ignore[reportArgumentType]
                ident,
                options=options,
                populate_existing=populate_existing,
                with_for_update=with_for_update,
                identity_token=identity_token,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )
            return entity.model_validate(instance) if instance else None
        else:
            return super().get(
                entity,
                ident,
                options=options,
                populate_existing=populate_existing,
                with_for_update=with_for_update,
                identity_token=identity_token,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )

    def get_one(
        self,
        entity: type[T],
        ident: _PKIdentityArgument,
        *,
        options: Optional[Sequence[ORMOption]] = None,
        populate_existing: bool = False,
        with_for_update: ForUpdateParameter = None,
        identity_token: Optional[Any] = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        bind_arguments: Optional[_BindArguments] = None,
    ) -> T:
        instance = self.get(
            entity,
            ident,
            options=options,
            populate_existing=populate_existing,
            with_for_update=with_for_update,
            identity_token=identity_token,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
        )

        if instance is None:
            raise exc.NoResultFound("No row was found when one was required")

        return instance

    def one(
        self,
        entity: type[T],
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(entity)

        if expressions:
            statement = statement.where(*expressions)
        if filters:
            statement = statement.filter_by(**filters)
        if options:
            statement = statement.options(*options)
        if execution_options:
            statement = statement.execution_options(**execution_options)

        return self.execute(statement).scalar_one()

    def one_or_none(
        self,
        entity: type[T],
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(entity)

        if expressions:
            statement = statement.where(*expressions)
        if filters:
            statement = statement.filter_by(**filters)
        if options:
            statement = statement.options(*options)
        if execution_options:
            statement = statement.execution_options(**execution_options)

        return self.execute(statement).scalar_one_or_none()

    def first(
        self,
        entity: type[T],
        order_bys: Iterable[_ColumnExpressionOrStrLabelArgument[Any]] | None = None,
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(entity)

        if order_bys:
            statement = statement.order_by(*order_bys)
        if expressions:
            statement = statement.where(*expressions)
        if filters:
            statement = statement.filter_by(**filters)
        if options:
            statement = statement.options(*options)
        if execution_options:
            statement = statement.execution_options(**execution_options)

        return self.execute(statement).scalars().first()

    def bulk(
        self,
        entity: type[T],
        idents: Sequence[_PKIdentityArgument],
        *,
        options: Sequence[ORMOption] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
    ) -> list[T | None]:
        """Bulk version of Session.get. Each element in idents should be
        exactly the same format as Session.get's ident parameter.

        Returns a list of entities in the same order as idents, with None
        for any ident that was not found.
        """
        if not idents:
            return []

        mapper = inspect(entity.__transmuter_provider__)
        pk_columns = mapper.primary_key  # pyright: ignore[reportOptionalMemberAccess]

        if len(pk_columns) == 1:
            # Build the WHERE clause based on single or composite PK
            pk_col = pk_columns[0]
            statement = select(entity).where(pk_col.in_(idents))
        else:
            # Composite PK: use tuple comparison
            # Each ident should be a tuple matching the PK columns
            statement = select(entity).where(tuple_(*pk_columns).in_(idents))

        if options:
            statement = statement.options(*options)
        if execution_options:
            statement = statement.execution_options(**execution_options)

        entities = self.execute(statement).scalars().all()

        # Build mapping from PK value(s) to entity
        if len(pk_columns) == 1:
            pk_attr = pk_columns[0].key
            mapping = {getattr(e, pk_attr): e for e in entities}
            return [mapping.get(ident) for ident in idents]
        else:
            # Composite PK: map tuple of PK values to entity
            pk_attrs = [col.key for col in pk_columns]
            mapping = {
                tuple(getattr(e, attr) for attr in pk_attrs): e for e in entities
            }
            return [
                mapping.get(tuple(ident) if not isinstance(ident, tuple) else ident)
                for ident in idents
            ]

    def count(
        self,
        entity: type[T],
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(functions.count()).select_from(entity)

        if expressions:
            statement = statement.where(*expressions)
        if filters:
            statement = statement.filter_by(**filters)
        if execution_options:
            statement = statement.execution_options(**execution_options)

        return self.execute(statement).scalar_one()

    def list(
        self,
        entity: type[T],
        limit: int | None = 100,
        offset: int | None = None,
        # cursor: UUID | None = None, # TODO: re-enable cursor pagination when identity solution is clarified
        order_bys: Iterable[_ColumnExpressionOrStrLabelArgument[Any]] | None = None,
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(entity)

        if limit:
            statement = statement.limit(limit)
        if offset:
            statement = statement.offset(offset)
        if order_bys:
            statement = statement.order_by(*order_bys)
        if options:
            statement = statement.options(*options)
        if expressions:
            statement = statement.where(*expressions)
        if execution_options:
            statement = statement.execution_options(**execution_options)
        if filters:
            statement = statement.filter_by(**filters)

        return self.execute(statement).scalars().all()

    def partitions(
        self,
        entity: type[T],
        limit: int | None = 100,
        offset: int | None = None,
        # cursor: UUID | None = None, # TODO: re-enable cursor pagination when identity solution is clarified
        size: int | None = 10,
        order_bys: Iterable[_ColumnExpressionOrStrLabelArgument[Any]] | None = None,
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        statement = select(entity).execution_options(yield_per=size)

        if limit:
            statement = statement.limit(limit)
        if offset:
            statement = statement.offset(offset)
        if order_bys:
            statement = statement.order_by(*order_bys)
        if options:
            statement = statement.options(*options)
        if expressions:
            statement = statement.where(*expressions)
        if execution_options:
            statement = statement.execution_options(**execution_options)
        if filters:
            statement = statement.filter_by(**filters)

        yield from self.execute(statement).scalars().partitions(size)


class AsyncSession(SqlalchemyAsyncSession):
    sync_session_class = Session
    sync_session: Session

    async def __aenter__(self) -> Self:
        self._validation_context_manager = validation_context(self._validation_context)
        self._validation_context_manager.__enter__()
        await super().__aenter__()
        return self

    async def __aexit__(self, type_: Any, value: Any, traceback: Any) -> None:
        if self._validation_context_manager is not None:
            self._validation_context_manager.__exit__(type_, value, traceback)
            self._validation_context_manager = None
        await super().__aexit__(type_, value, traceback)

    @property
    def _validation_context(self) -> ValidationContextT:
        return self.sync_session._validation_context

    @property
    def _validation_context_manager(self) -> ValidateContextGeneratorT | None:
        return self.sync_session._validation_context_manager

    @_validation_context_manager.setter
    def _validation_context_manager(self, value: ValidateContextGeneratorT | None):
        self.sync_session._validation_context_manager = value

    async def one(
        self,
        entity: type[T],
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        return await util.greenlet_spawn(
            self.sync_session.one,
            entity,
            options,
            expressions,
            execution_options,
            **filters,
        )

    async def one_or_none(
        self,
        entity: type[T],
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        return await util.greenlet_spawn(
            self.sync_session.one_or_none,
            entity,
            options,
            expressions,
            execution_options,
            **filters,
        )

    async def first(
        self,
        entity: type[T],
        order_bys: Iterable[_ColumnExpressionOrStrLabelArgument[Any]] | None = None,
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        return await util.greenlet_spawn(
            self.sync_session.first,
            entity,
            order_bys,
            options,
            expressions,
            execution_options,
            **filters,
        )

    async def bulk(
        self,
        entity: type[T],
        idents: Sequence[_PKIdentityArgument],
        *,
        options: Sequence[ORMOption] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
    ) -> list[T | None]:
        return await util.greenlet_spawn(
            self.sync_session.bulk,
            entity,
            idents,
            options=options,
            execution_options=execution_options,
        )

    async def count(
        self,
        entity: type[T],
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        return await util.greenlet_spawn(
            self.sync_session.count,
            entity,
            expressions,
            execution_options,
            **filters,
        )

    async def list(
        self,
        entity: type[T],
        limit: int | None = 100,
        offset: int | None = None,
        order_bys: Iterable[_ColumnExpressionOrStrLabelArgument[Any]] | None = None,
        options: Iterable[ExecutableOption] | None = None,
        expressions: Iterable[ExpressionT] | None = None,
        execution_options: OrmExecuteOptionsParameter = util.EMPTY_DICT,
        **filters,
    ):
        return await util.greenlet_spawn(
            self.sync_session.list,
            entity,
            limit,
            offset,
            order_bys,
            options,
            expressions,
            execution_options,
            **filters,
        )
