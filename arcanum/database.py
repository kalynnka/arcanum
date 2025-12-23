from typing import Any, Iterable

from sqlalchemy.orm import Session as SqlalchemySession
from sqlalchemy.sql.selectable import ForUpdateArg

from arcanum.base import BaseProtocol
from arcanum.result import _TP, AdaptedResult
from arcanum.selectable import AdaptedSelect


class Session(SqlalchemySession):
    def execute(self, statement: AdaptedSelect[_TP]) -> AdaptedResult[_TP]:
        return AdaptedResult(
            real_result=super().execute(statement),
            adapter=statement.adapter,
            scalar_adapter=statement.scalar_adapter,
        )

    def refresh(
        self,
        instance: object,
        attribute_names: Iterable[str] | None = None,
        with_for_update: ForUpdateArg | None | bool | dict[str, Any] = None,
    ) -> None:
        super().refresh(instance, attribute_names, with_for_update)
        if isinstance(instance, BaseProtocol):
            instance.__pydantic_validator__.validate_python(
                instance.__provided__,
                self_instance=instance,
            )
