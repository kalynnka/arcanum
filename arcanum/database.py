from sqlalchemy.orm import Session as SqlalchemySession

from arcanum.result import _TP, AdaptedResult
from arcanum.sql import AdaptedSelect


class Session(SqlalchemySession):
    def execute(self, statement: AdaptedSelect[_TP]) -> AdaptedResult[_TP]:
        return AdaptedResult(
            real_result=super().execute(statement),
            scalar_adapter=statement.scalar_adapter,
            scalars_adapter=statement.scalars_adapter,
        )
