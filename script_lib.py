r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.migrate_fact_upload.title
    'append data from a workspace table.'

    >>> text = Script.migrate_fact_upload.value
    >>> lines = text.split('\n')
    >>> print(lines[0])
    ... #doctest: +NORMALIZE_WHITESPACE
    /** migrate_fact_upload - append data from a workspace table.

We can separate the script into statements::

    >>> statements = Script.epic_flowsheets_transform.statements()
    >>> print(next(s for s in statements if 'insert' in s))
    ... #doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    insert into etl_test_values (test_value, test_domain, test_name, result_id, result_date)
    with test_key as ( ...

A bit of sqlplus syntax is supported for ignoring errors in just part
of a script:

    >>> Script.sqlerror('whenever sqlerror exit')
    False
    >>> Script.sqlerror('whenever sqlerror continue')
    True
    >>> Script.sqlerror('select 1 + 1 from dual') is None
    True

Dependencies between scripts are declared as follows::

    >>> print(next(decl for decl in statements if "'dep'" in decl))
    ... #doctest: +ELLIPSIS
    select test_name from etl_tests where 'dep' = 'etl_tests_init.sql'

    >>> Script.epic_flowsheets_transform.deps()
    ... #doctest: +ELLIPSIS
    [<Script(etl_tests_init)>]

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.epic_flowsheets_transform.created_objects()
    ... #doctest: +ELLIPSIS
    [view etl_test_domain_flowsheets, view flo_meas_type, view flowsheet_day, ...

as well as tables inserted into::

    >>> variables={I2B2STAR: 'I2B2DEMODATA',
    ...            CMS_RIF: 'CMS_DEID', 'upload_id': '20', 'chunk_qty': 20,
    ...            'cms_source_cd': "'ccwdata.org'", 'source_table': 'T'}
    >>> Script.epic_flowsheets_transform.inserted_tables(variables)
    ['etl_test_values', 'approved_deid_flowsheets', 'etl_test_values']

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print(statements[-1])
    ... #doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    select 1 up_to_date
    from epic_flowsheets_txform_sql where design_digest = &&design_digest

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.epic_flowsheets_transform.digest()
    >>> last = Script.epic_flowsheets_transform.statements(variables)[-1].strip()
    >>> print(last)
    ... #doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    select 1 up_to_date
    from epic_flowsheets_txform_sql where design_digest = ...

ISSUE : Python hashes are senstive to the machine running the test?

Some scripts use variables that are not known until a task is run; for
example, `&&upload_id` is used in names of objects such as tables and
partitions; these scripts must not refer to such variables in their
completion query:

    >>> del variables['upload_id']
    >>> print(Script.migrate_fact_upload.statements(variables,
    ...     skip_unbound=True)[-1].strip())
    commit

'''

from itertools import groupby
from typing import Dict, Iterable, List, Optional, Sequence, Text, Tuple, Type
from zlib import adler32
import enum
import re
import abc

import pkg_resources as pkg

import sql_syntax
from sql_syntax import (
    Environment, StatementInContext, ObjectId, SQL, Name,
    iter_statement)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts
CMS_RIF = 'CMS_RIF'

ScriptStep = Tuple[int, Text, SQL]
Filename = str


class SQLMixin(enum.Enum):
    @property
    def sql(self) -> SQL:
        from typing import cast
        return cast(SQL, self.value)  # hmm...

    @property
    def fname(self) -> str:
        return self.name + self.extension

    @abc.abstractproperty
    def extension(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        raise NotImplementedError

    def each_statement(self,
                       variables: Optional[Environment]=None,
                       skip_unbound: bool=False) -> Iterable[ScriptStep]:
        for line, comment, statement in self.parse(self.sql):
            try:
                ss = sql_syntax.substitute(statement, self._all_vars(variables))
            except KeyError:
                if skip_unbound:
                    continue
                else:
                    raise
            yield line, comment, ss

    def _all_vars(self, variables: Optional[Environment]) -> Optional[Environment]:
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=str(self.digest()))

    def statements(self,
                   variables: Optional[Environment]=None,
                   skip_unbound: bool=False) -> Sequence[Text]:
        return list(stmt for _l, _c, stmt
                    in self.each_statement(skip_unbound=skip_unbound,
                                           variables=variables))

    def created_objects(self) -> List[ObjectId]:
        return []

    def inserted_tables(self,
                        variables: Environment={}) -> List[Name]:
        return []

    @property
    def title(self) -> Text:
        line1 = self.sql.split('\n', 1)[0]
        if not (line1.startswith('/** ') and ' - ' in line1):
            raise ValueError('%s missing title block' % self)
        return line1.split(' - ', 1)[1].strip()

    def deps(self) -> List['SQLMixin']:
        return [child
                for sql in self.statements()
                for child in Script._get_deps(sql)]

    def dep_closure(self) -> List['SQLMixin']:
        return [self] + [descendant
                         for sql in self.statements()
                         for child in Script._get_deps(sql)
                         for descendant in child.dep_closure()]

    def digest(self) -> int:
        '''Hash the text of this script and its dependencies.

        Unlike the python hash() function, this digest is consistent across runs.
        '''
        return adler32(str(self._text()).encode('utf-8'))

    def _text(self) -> List[str]:
        '''Get the text of this script and its dependencies.

        >>> nodeps = Script.migrate_fact_upload
        >>> nodeps._text() == [nodeps.value]
        True

        >>> complex = Script.epic_flowsheets_transform
        >>> complex._text() != [complex.value]
        True
        '''
        return sorted(set(s.sql for s in self.dep_closure()))

    @classmethod
    def _get_deps(cls, sql: Text) -> List['SQLMixin']:
        '''
        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'oops.sql'")
        Traceback (most recent call last):
            ...
        KeyError: 'oops'

        >>> Script._get_deps(
        ...     "select col from t where x = 'name.sql'")
        []
        '''
        from typing import cast

        m = re.search(r"select \S+ from \S+ where 'dep' = '([^']+)'", sql)
        if not m:
            return []
        name, ext = m.group(1).rsplit('.', 1)
        choices = Script if ext == 'sql' else []
        deps = [cast(SQLMixin, s) for s in choices if s.name == name]
        if not deps:
            raise KeyError(name)
        return deps

    @classmethod
    def sqlerror(cls, s: SQL) -> Optional[bool]:
        if s.strip().lower() == 'whenever sqlerror exit':
            return False
        elif s.strip().lower() == 'whenever sqlerror continue':
            return True
        return None


class ScriptMixin(SQLMixin):
    @property
    def extension(self) -> str:
        return '.sql'

    def parse(self, text: SQL,
              block_sep: str='/') -> Iterable[StatementInContext]:
        lines = [l.strip() for l in text.split('\n')]
        return (sql_syntax.iter_blocks(text) if block_sep in lines
                else iter_statement(text))

    def created_objects(self) -> List[ObjectId]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in sql_syntax.created_objects(stmt)]

    def inserted_tables(self,
                        variables: Optional[Environment]={}) -> List[Name]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in sql_syntax.inserted_tables(
                        sql_syntax.substitute(stmt, self._all_vars(variables)))]


class Script(ScriptMixin, enum.Enum):
    '''Script is an enum.Enum of contents.

    ISSUE: It's tempting to consider separate libraries for NAACCR,
           NTDS, etc., but that doesn't integrate well with the
           generic luigi.EnumParameter() in etl_tasks.SqlScriptTask.

    '''
    [
        # Keep sorted
        condition,
        death,
        death_cause,
        demographic,
        diagnosis,
        dispensing,
        encounter,
        enrollment,
        harvest,
        lab_result_cm,
        med_admin,
        med_admin_init,
        obs_clin,
        obs_gen,
        pcornet_init,
        pcornet_loader,
        pcornet_trial,
        prescribing,
        pro_cm,
        procedures,
        provider,
        vital
    ] = [
        pkg.resource_string(__name__,
                            'Oracle/' + fname).decode('utf-8')
        for fname in [
                'condition.sql',
                'death.sql',
                'death_cause.sql',
                'demographic.sql',
                'diagnosis.sql',
                'dispensing.sql',
                'encounter.sql',
                'enrollment.sql',
                'harvest.sql',
                'lab_result_cm.sql',
                'med_admin.sql',
                'med_admin_init.sql',
                'obs_clin.sql',
                'obs_gen.sql',
                'pcornet_init.sql',
                'pcornet_loader.sql',
                'pcornet_trial.sql',
                'prescribing.sql',
                'pro_cm.sql',
                'procedures.sql',
                'provider.sql',
                'vital.sql'
        ]
    ]

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


def _object_to_creators(libs: List[Type[SQLMixin]]) -> Dict[ObjectId, List[SQLMixin]]:
    '''Find creator scripts for each object.

    "There can be only one."
    >>> creators = _object_to_creators([Script])
    >>> [obj for obj, scripts in creators.items()
    ...  if len(scripts) > 1]
    []
    '''
    fst = lambda pair: pair[0]
    snd = lambda pair: pair[1]

    objs = sorted(
        [(obj, s)
         for lib in libs for s in lib
         for obj in s.created_objects()],
        key=fst)
    by_obj = groupby(objs, key=fst)
    return dict((obj, list(map(snd, places))) for obj, places in by_obj)


_redefined_objects = [
    obj for obj, scripts in _object_to_creators([Script]).items()
    if len(scripts) > 1]
assert _redefined_objects == [], _redefined_objects
