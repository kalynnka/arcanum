from typing import Any, TypeVar

from sqlalchemy.orm import Session as SqlalchemySession

from arcanum.result import AdaptedResult
from arcanum.sql import AdaptedSelect

TP = TypeVar("TP", bound=tuple[Any, ...])


class Session(SqlalchemySession):
    def execute(self, statement: AdaptedSelect[TP]) -> AdaptedResult[TP]:
        return AdaptedResult(
            real_result=super().execute(statement),
            scalar_adapter=statement.scalar_adapter,
            scalars_adapter=statement.scalars_adapter,
        )
