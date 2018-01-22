# TODO: module doc: luigi's abuse of class attributes vs. mypy static typing
from datetime import datetime
from typing import Any, Callable, TypeVar, cast

import luigi


_T = TypeVar('_T')
_U = TypeVar('_U')


def _valueOf(example: _T, cls: Callable[..., _U]) -> Callable[..., _T]:

    def getValue(*args: Any, **kwargs: Any) -> _T:
        return cast(_T, cls(*args, **kwargs))
    return getValue


class TimeStampParameter(luigi.Parameter):
    '''A datetime interchanged as milliseconds since the epoch.
    '''

    def parse(self, s: str) -> datetime:
        ms = int(s)
        return datetime.fromtimestamp(ms / 1000.0)

    def serialize(self, dt: datetime) -> str:
        epoch = datetime.utcfromtimestamp(0)
        ms = (dt - epoch).total_seconds() * 1000
        return str(int(ms))


StrParam = _valueOf('s', luigi.Parameter)
IntParam = _valueOf(0, luigi.IntParameter)
BoolParam = _valueOf(True, luigi.BoolParameter)
DictParam = _valueOf({'k': 'v'}, luigi.DictParameter)
TimeStampParam = _valueOf(datetime(2001, 1, 1, 0, 0, 0), TimeStampParameter)
