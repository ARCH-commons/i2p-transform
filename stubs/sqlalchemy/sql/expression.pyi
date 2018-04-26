from .elements import ClauseElement

class Select(ClauseElement):
    @property
    def columns(self): ...
    c = ... # type: Any
    def where(self, whereclause): ...
