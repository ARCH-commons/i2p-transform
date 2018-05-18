from collections import defaultdict
from csv import DictReader
from etl_tasks import CDMStatusTask
from param_val import StrParam
from typing import Dict

from sqlalchemy import func, MetaData, Table, Column  # type: ignore
from sqlalchemy.types import String  # type: ignore


import logging

log = logging.getLogger(__name__)

class LoadCSV(CDMStatusTask):
    csvname = StrParam()

    def run(self) -> None:
        self.setTaskStart()
        self.load()
        self.setTaskEnd(self.getRecordCount())

    def load(self) -> None:
        def sz(l: int, chunk: int=16) -> int:
            return max(chunk, chunk * ((l + chunk - 1) // chunk))

        db = self._dbtarget().engine
        schema = MetaData()
        l = list()

        with open(self.csvname) as fin:  # ISSUE: ambient
            dr = DictReader(fin)

            Dict  # for tools that don't see type: comments.
            mcl = defaultdict(int)  # type: Dict[str, int]
            for row in dr:
                l.append(row)
                for col in dr.fieldnames:
                    mcl[col] = sz(max(mcl[col], len(row[col])))

            columns = ([Column(n, String(mcl[n])) for n in dr.fieldnames])
            table = Table(self.taskName, schema, *columns)
            if table.exists(bind=db):
                table.drop(db)
            table.create(db)

            db.execute(table.insert(), l)
