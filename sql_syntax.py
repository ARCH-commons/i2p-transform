'''sql_syntax - break SQL scripts into statements, etc.

'''

from datetime import datetime
from typing import Dict, Iterable, List, Optional, Text, Tuple, Union
import re

Name = Text
SQL = Text
Environment = Dict[Name, Text]
Params = Dict[str, Union[str, int, datetime]]
Line = int
Comment = Text
StatementInContext = Tuple[Line, Comment, SQL]


def iter_statement(txt: SQL) -> Iterable[StatementInContext]:
    r'''Iterate over SQL statements in a script.

    >>> list(iter_statement("drop table foo; create table foo"))
    [(1, '', 'drop table foo'), (1, '', 'create table foo')]

    >>> list(iter_statement("-- blah blah\ndrop table foo"))
    [(2, '-- blah blah\n', 'drop table foo')]

    >>> list(iter_statement("drop /* blah blah */ table foo"))
    [(1, '', 'drop  table foo')]

    >>> list(iter_statement("select '[^;]+' from dual"))
    [(1, '', "select '[^;]+' from dual")]

    >>> list(iter_statement('select "x--y" from z;'))
    [(1, '', 'select "x--y" from z')]
    >>> list(iter_statement("select 'x--y' from z;"))
    [(1, '', "select 'x--y' from z")]
    '''

    statement = comment = ''
    line = 1
    sline = None  # type: Optional[int]

    def save(txt: SQL) -> Tuple[SQL, Optional[int]]:
        return (statement + txt, sline or (line if txt else None))

    while 1:
        m = SQL_SEPARATORS.search(txt)
        if not m:
            statement, sline = save(txt)
            break

        pfx, match, txt = (txt[:m.start()],
                           txt[m.start():m.end()],
                           txt[m.end():])
        if pfx:
            statement, sline = save(pfx)

        if m.group('sep'):
            if sline:
                yield sline, comment, statement
            statement = comment = ''
            sline = None
        elif [n for n in ('lit', 'hint', 'sym')
              if m.group(n)]:
            statement, sline = save(match)
        elif (m.group('space') and statement):
            statement, sline = save(match)
        elif ((m.group('comment') and not statement) or
              (m.group('space') and comment)):
            comment += match

        line += (pfx + match).count("\n")

    if sline and (comment or statement):
        yield sline, comment, statement


# Check for hint before comment since a hint looks like a comment
SQL_SEPARATORS = re.compile(
    r'(?P<space>^\s+)'
    r'|(?P<hint>/\*\+.*?\*/)'
    r'|(?P<comment>(--[^\n]*(?:\n|$))|(?:/\*([^\*]|(\*(?!/)))*\*/))'
    r'|(?P<sym>"[^\"]*")'
    r"|(?P<lit>'[^\']*')"
    r'|(?P<sep>;)')


def _test_iter_statement() -> None:
    r'''
    >>> list(iter_statement("/* blah blah */ drop table foo"))
    [(1, '/* blah blah */ ', 'drop table foo')]

    >>> [l for (l, c, s) in iter_statement('s1;\n/**********************\n'
    ...                     '* Medication concepts \n'
    ...                     '**********************/\ns2')]
    [1, 5]

    >>> list(iter_statement(""))
    []

    >>> list(iter_statement("/*...*/   "))
    []

    >>> list(iter_statement("drop table foo;   "))
    [(1, '', 'drop table foo')]

    >>> list(iter_statement("drop table foo"))
    [(1, '', 'drop table foo')]

    >>> list(iter_statement("/* *** -- *** */ drop table foo"))
    [(1, '/* *** -- *** */ ', 'drop table foo')]

    >>> list(iter_statement("/* *** -- *** */ drop table x  /* ... */"))
    [(1, '/* *** -- *** */ ', 'drop table x  ')]

    >>> list(iter_statement('select /*+ index(ix1 ix2) */ * from some_table'))
    [(1, '', 'select /*+ index(ix1 ix2) */ * from some_table')]

    >>> len(list(iter_statement(""" /* + no space before + in hints*/
    ...  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */
    ...  select * from some_table;  """))[0][2])
    79

    >>> list(iter_statement('select 1+1; /* nothing else */; '))
    [(1, '', 'select 1+1')]
    '''
    pass  # pragma: nocover


def substitute(sql: SQL, variables: Optional[Environment]) -> SQL:
    '''Evaluate substitution variables in the style of Oracle sqlplus.

    >>> substitute('select &&not_bound from dual', {})
    Traceback (most recent call last):
    KeyError: 'not_bound'
    '''
    if variables is None:
        return sql
    sql_esc = sql.replace('%', '%%')  # escape %, which we use specially
    return re.sub('&&(\w+)', r'%(\1)s', sql_esc) % variables


def params_used(params: Params, statement: SQL) -> Params:
    return dict((k, v) for (k, v) in params.items()
                if k in param_names(statement))


def param_names(s: SQL) -> List[Name]:
    '''
    >>> param_names('select 1+1 from dual')
    []
    >>> param_names('select 1+:y from dual')
    ['y']
    '''
    return [expr[1:]
            for expr in re.findall(r':\w+', s)]


def first_cursor(statement: SQL) -> SQL:
    '''Find argument of first obvious call to `cursor()`.

    >>> first_cursor('select * from table(f(cursor(select * from there)))')
    'select * from there'
    '''
    return statement.split('cursor(')[1].split(')')[0]


class ObjectId(object):
    kind = ''

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return '{} {}'.format(self.kind, self.name)

    def __hash__(self) -> int:
        return hash((self.kind, self.name))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectId):
            return False
        return (self.kind, self.name) == ((other.kind, other.name))

    def __lt__(self, other: 'ObjectId') -> bool:
        return (self.kind, self.name) < ((other.kind, other.name))


class TableId(ObjectId):
    kind = 'table'


class ViewId(ObjectId):
    kind = 'view'


def created_objects(statement: SQL) -> List[ObjectId]:
    r'''
    >>> created_objects('create table t as ...')
    [table t]

    >>> created_objects('create or replace view x\nas ...')
    [view x]
    '''
    m = re.search('^create or replace view (\S+)', statement.strip())
    views = [ViewId(m.group(1))] if m else []  # type: List[ObjectId]
    m = re.search('^create table (\S+)', statement.strip())
    tables = [TableId(m.group(1))] if m else []  # type: List[ObjectId]
    return tables + views


def inserted_tables(statement: SQL) -> List[Name]:
    r'''
    >>> inserted_tables('create table t as ...')
    []
    >>> inserted_tables('insert into t (...) ...')
    ['t']
    '''
    if not statement.startswith('insert'):
        return []
    m = re.search('into\s+(\S+)', statement.strip())
    return [m.group(1)] if m else []


def insert_append_table(statement: SQL) -> Optional[Name]:
    if '/*+ append' in statement:
        [t] = inserted_tables(statement)
        return t
    return None


def iter_blocks(module: SQL,
                separator: str ='\n/\n') -> Iterable[StatementInContext]:
    line = 1
    for block in module.split(separator):
        if block.strip():
            yield (line, '...no comment handling...', block)
        line += len((block + separator).split('\n')[:-1])
