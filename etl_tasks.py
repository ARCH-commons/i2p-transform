'''etl_tasks -- Source-agnostic Luigi ETL Task support

Note: This is source-agnostic but not target-agnositc; it has i2b2
      knowledge.

'''

from typing import Any, Callable, Dict, Iterator, List, Optional as Opt, Tuple, cast
from contextlib import contextmanager
from datetime import datetime
import csv
import logging

from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text as sql_text, Column, func, MetaData, Table  # type: ignore
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.expression import Select
import sqlalchemy as sqla
import luigi
from cx_Oracle import Error as OraError, _Error as Ora_Error

from eventlog import EventLogger, LogState, JSONObject  # ISSUE: use https://eliot.readthedocs.io/en/1.1.0/ instead?
from param_val import StrParam, IntParam, BoolParam
from script_lib import Script
from sql_syntax import Environment, Params, SQL
from sql_syntax import params_used, insert_append_table

log = logging.getLogger(__name__)


class DBTarget(SQLAlchemyTarget):
    '''Take advantage of engine caching logic from SQLAlchemyTarget,
    but don't bother with target_table, update_id, etc.

    >>> t = DBTarget(connection_string='sqlite:///')
    >>> t.engine.scalar('select 1 + 1')
    2
    '''
    def __init__(self, connection_string: str,
                 target_table: Opt[str]=None, update_id: Opt[str]=None,
                 echo: bool=False) -> None:
        SQLAlchemyTarget.__init__(
            self, connection_string,
            target_table=target_table,
            update_id=update_id,
            echo=echo)

    def exists(self) -> bool:
        raise NotImplementedError

    def touch(self) -> None:
        raise NotImplementedError


class ETLAccount(luigi.Config):
    '''Access to connect and run ETL.

    This account needs read access to source material, write access to
    the destination i2b2 star schema, and working space for
    intermediary tables, views, functions, and such.
    '''
    account = StrParam(description='see client.cfg',
                       default='')
    passkey = StrParam(description='see client.cfg',
                       default='',
                       significant=False)
    ssh_tunnel = StrParam(description='see client.cfg',
                          default='',
                          significant=False)
    echo = BoolParam(description='SQLAlchemy echo logging',
                     significant=False)


class LoggedConnection(object):
    '''Wrap (parts of) the sqlalchemy Connection API with logging.

    If you have an EventLogger, `log`, you can wrap a connection
    in a step that explains what the connection is for::

        with log.step('crunching data') as step:
            lc = LoggedConnection(conn, log, step)

    .. note:: A LoggedConnection wraps only the `execute` and `scalar`
              methods from the sqlalchemy API.

    '''
    def __init__(self, conn: Connection, log: EventLogger,
                 step: LogState) -> None:
        self._conn = conn
        self.log = log
        self.step = step

    def __repr__(self) -> str:
        return '%s(%s, %s)' % (self.__class__.__name__, self._conn, self.log)

    def _log_args(self, event: str, operation: object,
                  params: Params) -> Tuple[str, JSONObject, JSONObject]:
        msg = '%(event)s %(sql3)s' + ('\n%(params)s' if params else '')
        argobj = dict(event=event, sql3=_peek(operation, lines=3), params=params)
        extra = dict(statement=str(operation))
        return msg, argobj, extra

    def execute(self, operation: object, params: Opt[Params] = None) -> ResultProxy:
        msg, argobj, extra = self._log_args('execute', operation, params or {})
        with self.log.step(msg, argobj, extra):
            return self._conn.execute(operation, params or {})

    def scalar(self, operation: object, params: Opt[Params] = None) -> Any:
        msg, argobj, extra = self._log_args('scalar', str(operation), params or {})
        with self.log.step(msg, argobj, extra) as step:
            result = self._conn.scalar(operation, params or {})
            step.extra.update(dict(result=result))
            return result


def _peek(thing: object,
          lines: int=1,
          max_len: int=120) -> str:
    return '\n'.join(str(thing).split('\n')[:lines])[:180]


class DBAccessTask(luigi.Task):
    """Manage DB account credentials and logging.

    Typical usage::

        with self.connection(event='munching data') as conn:
            howmany = conn.scalar('select count(*) from cookies')
            yummy = conn.execute('select * from cookies')
    """
    account = StrParam(default=ETLAccount().account)
    ssh_tunnel = StrParam(default=ETLAccount().ssh_tunnel,
                          significant=False)
    passkey = StrParam(default=ETLAccount().passkey,
                       significant=False)
    echo = BoolParam(default=ETLAccount().echo,
                     significant=False)
    max_idle = IntParam(description='Set to less than Oracle profile max idle time.',
                        default=60 * 20,
                        significant=False)
    _log = logging.getLogger(__name__)  # ISSUE: ambient.

    def output(self) -> luigi.Target:
        return self._dbtarget()

    def _dbtarget(self) -> DBTarget:
        return DBTarget(self._make_url(self.account),
                        target_table=None, update_id=self.task_id,
                        echo=self.echo)

    def _make_url(self, account: str) -> str:
        url = make_url(account)
        if 'oracle' in account.lower():
            url.query['pool_recycle'] = self.max_idle
            # `twophase` interferes with direct path load somehow.
            # To disable twophase, allow_twophase flag was commented out of opts dict in cx_Oracle.py
            # This appears to be a change from cx_Oracle version 5 to 6.
            url.query['allow_twophase'] = False
        if self.passkey:
            from os import environ  # ISSUE: ambient
            url.password = environ[self.passkey]
        if self.ssh_tunnel and url.host:
            host, port = self.ssh_tunnel.split(':', 1)
            url.host = host
            url.port = port
        return str(url)

    def log_info(self) -> Dict[str, Any]:
        '''Get info to log: luigi params, task_family, task_hash.
        '''
        return dict(self.to_str_params(only_significant=True),
                    task_family=self.task_family,
                    task_hash=self.task_id[-luigi.task.TASK_ID_TRUNCATE_HASH:])

    @contextmanager
    def connection(self, event: str='connect') -> Iterator[LoggedConnection]:
        conn = ConnectionProblem.tryConnect(self._dbtarget().engine)
        log = EventLogger(self._log, self.log_info())
        with log.step('%(event)s: <%(account)s>',
                      dict(event=event, account=self.account)) as step:
            yield LoggedConnection(conn, log, step)

    def _fix_password(self, environ: Dict[str, str], getpass: Callable[[str], str]) -> None:
        '''for interactive use; e.g. in notebooks
        '''
        if self.passkey not in environ:
            environ[self.passkey] = getpass(self.passkey)


class SqlScriptTask(DBAccessTask):
    '''Task to run a stylized SQL script.

    As seen in `script_lib`, a script may require (in the luigi sense)
    other scripts and it  is complete iff its last query says so.

    Running a script may be parameterized with bind params and/or
    Oracle sqlplus style defined `&&variables`:

    >>> variables = dict(I2B2STAR='I2B2DEMODATA')
    >>> txform = SqlScriptTask(
    ...    account='sqlite:///', passkey=None,
    ...    script=Script.pcornet_loader,
    ...    param_vars=variables)

    ISSUE: doctest dependencies?
    >>> [task.script for task in txform.requires()]
    ... #doctest: +ELLIPSIS
    []

    >>> txform.complete()
    False

    '''
    script = cast(Script, luigi.EnumParameter(enum=Script))
    param_vars = cast(Environment, luigi.DictParameter(default={}))
    _log = logging.getLogger('sql_scripts')  # ISSUE: ambient. magic-string

    @property
    def variables(self) -> Environment:
        '''Defined variables for this task (or task family).
        '''
        return self.param_vars

    @property
    def vars_for_deps(self) -> Environment:
        '''Defined variables to supply to dependencies.
        '''
        return self.variables

    def requires(self) -> List[luigi.Task]:
        '''Wrap each of `self.script.deps()` in a SqlScriptTask.
        '''
        return [type(s.name, (SqlScriptTask,), {'script':s,
                              'param_vars':self.vars_for_deps,
                              'account':self.account,
                              'passkey':self.passkey,
                              'echo':self.echo})()
                for s in self.script.deps()]

    def log_info(self) -> Dict[str, Any]:
        '''Include script, filename in self.log_info().
        '''
        return dict(DBAccessTask.log_info(self),
                    script=self.script.name,
                    filename=self.script.fname)

    def complete(self) -> bool:
        '''Each script's last query tells whether it is complete.

        It should be a scalar query that returns non-zero for done
        and either zero or an error for not done.
        '''
        last_query = self.last_query()
        params = params_used(self.complete_params(), last_query)
        with self.connection(event=self.task_family + ' complete query: ' + self.script.name) as conn:
            try:
                result = conn.scalar(sql_text(last_query), params)
                return bool(result)
            except DatabaseError as exc:
                conn.log.warning('%(event)s: %(exc)s',
                                 dict(event='complete query error', exc=exc))
                return False

    def last_query(self) -> SQL:
        """
        Note: In order to support run-only variables as in UploadTask,
              we skip statements with unbound &&variables.
        """
        return self.script.statements(
            skip_unbound=True,
            variables=self.variables)[-1]

    def complete_params(self) -> Dict[str, Any]:
        '''Make `task_id` available to complete query as a bind param.
        '''
        return dict(task_id=self.task_id)

    def run(self) -> None:
        '''Run each statement in the script without any bind parameters.
        '''
        self.run_bound()

    def run_bound(self,
                  script_params: Opt[Params]=None) -> None:
        '''Run with a (default emtpy) set of parameters bound.
        '''
        with self.connection(event='run script') as conn:
            self.run_event(conn, script_params=script_params)

    def run_event(self,
                  conn: LoggedConnection,
                  run_vars: Opt[Environment]=None,
                  script_params: Opt[Params]=None) -> int:
        '''Run script inside a LoggedConnection event.

        @param run_vars: variables to define for this run
        @param script_params: parameters to bind for this run
        @return: count of rows bulk-inserted
                 always 0 for this class, but see UploadTask

        To see how a script can ignore errors, see :mod:`script_lib`.
        '''
        bulk_rows = 0
        ignore_error = False
        run_params = dict(script_params or {}, task_id=self.task_id)
        fname = self.script.fname
        variables = dict(run_vars or {}, **self.variables)
        each_statement = self.script.each_statement(variables=variables)

        for line, _comment, statement in each_statement:
            try:
                if self.is_bulk(statement):
                    bulk_rows = self.bulk_insert(
                        conn, fname, line, statement, run_params,
                        bulk_rows)
                else:
                    ignore_error = self.execute_statement(
                        conn, fname, line, statement, run_params,
                        ignore_error)
            except DatabaseError as exc:
                db = self._dbtarget().engine
                err = SqlScriptError(exc, self.script, line,
                                     statement, str(db))
                if ignore_error:
                    conn.log.warning('%(event)s: %(error)s',
                                     dict(event='ignore', error=err))
                else:
                    raise err from None
        if bulk_rows > 0:
            conn.step.msg_parts.append(' %(rowtotal)s total rows')
            conn.step.argobj.update(dict(rowtotal=bulk_rows))

        return bulk_rows

    def execute_statement(self, conn: LoggedConnection, fname: str, line: int,
                          statement: SQL, run_params: Params,
                          ignore_error: bool) -> bool:
        '''Log and execute one statement.
        '''
        sqlerror = Script.sqlerror(statement)
        if sqlerror is not None:
            return sqlerror
        params = params_used(run_params, statement)
        self.set_status_message(
            '%s:%s:\n%s\n%s' % (fname, line, statement, params))
        conn.execute(statement, params)
        return ignore_error

    def is_bulk(self, statement: SQL) -> bool:
        '''always False for this class, but see UploadTask
        '''
        return False

    def bulk_insert(self, conn: LoggedConnection, fname: str, line: int,
                    statement: SQL, run_params: Params,
                    bulk_rows: int) -> int:
        raise NotImplementedError(
            'overriding is_bulk() requires overriding bulk_insert()')


class CDMStatusTask(DBAccessTask):
    taskName = StrParam()
    expectedRecords = IntParam(default=1)

    def complete(self) -> bool:
        with self.connection() as q:
            actualRecords = q.scalar('select records from cdm_status where task = \'%s\'' % self.taskName)
            actualRecords = 0 if actualRecords == None else actualRecords
            log.info('task %s has %d rows', self.taskName, actualRecords)
            return actualRecords >= self.expectedRecords  # type: ignore  # sqla

    def getRecordCount(self) -> int:
        with self.connection() as q:
            return q.scalar(sqla.select([func.count()]).select_from(self.taskName))

    def setTaskEnd(self, rowCount: int):
        statusTable = Table("cdm_status", MetaData(), Column('TASK'), Column('START_TIME'), Column('END_TIME'), Column('RECORDS'))

        db = self._dbtarget().engine
        db.execute(statusTable.update, [{'END_TIME': datetime.now(), 'RECORDS': rowCount}])

    def setTaskStart(self) -> None:
        statusTable = Table("cdm_status", MetaData(), Column('TASK'), Column('START_TIME'), Column('END_TIME'), Column('RECORDS'))

        db = self._dbtarget().engine
        db.execute(statusTable.insert(), [{'TASK': self.taskName, 'START_TIME': datetime.now()}])


def log_plan(lc: LoggedConnection, event: str, params: Dict[str, Any],
             query: Opt[Select]=None, sql: Opt[str]=None) -> None:
    if query is not None:
        sql = str(query.compile(bind=lc._conn))
    if sql is None:
        return
    plan = explain_plan(lc, sql)
    param_msg = ', '.join('%%(%s)s' % k for k in params.keys())
    lc.log.info('%(event)s [' + param_msg + ']\n'
                'query: %(query_peek)s plan:\n'
                '%(plan)s',
                dict(params, event=event, query_peek=_peek(sql),
                     plan='\n'.join(plan)))


def explain_plan(work: LoggedConnection, statement: SQL) -> List[str]:
    work.execute('explain plan for ' + statement)
    # ref 19 Using EXPLAIN PLAN
    # Oracle 10g Database Performance Tuning Guide
    # https://docs.oracle.com/cd/B19306_01/server.102/b14211/ex_plan.htm
    plan = work.execute(
        'SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY())')
    return [row.line for row in plan]  # type: ignore  # sqla


def maybe_ora_err(exc: Exception) -> Opt[Ora_Error]:
    if isinstance(exc, DatabaseError):
        if isinstance(exc.orig, OraError):
            return cast(Ora_Error, exc.orig.args[0])
    return None


class SqlScriptError(IOError):
    '''Include script file, line number in diagnostics
    '''
    def __init__(self, exc: Exception, script: Script, line: int, statement: SQL,
                 conn_label: str) -> None:
        fname = script.name
        message = '%s <%s>\n%s:%s:\n'
        args = [exc, conn_label, fname, line]
        ora_ex = maybe_ora_err(exc)
        if ora_ex:
            offset = ora_ex.offset
            message += '%s<ERROR>%s'
            args[0] = ora_ex.message
            args += [_pick_lines(statement[:offset], -3, None),
                     _pick_lines(statement[offset:], None, 3)]
        else:
            message += '%s'
            args += [statement]

        self.message = message
        self.args = tuple(args)

    def __str__(self) -> str:
        return self.message % self.args


def _pick_lines(s: str, lo: Opt[int], hi: Opt[int]) -> str:
    return '\n'.join(s.split('\n')[lo:hi])


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


class SourceTask(luigi.Task):
    @property
    def source_cd(self) -> str:
        raise NotImplementedError

    @property
    def download_date(self) -> datetime:
        raise NotImplementedError


class I2B2Task(object):
    @property
    def project(self) -> 'I2B2ProjectCreate':
        return I2B2ProjectCreate()


class UploadTask(I2B2Task, SqlScriptTask):
    '''Run a script with an associated `upload_status` record.
    '''
    @property
    def source(self) -> SourceTask:
        raise NotImplementedError('subclass must implement')

    @property
    def transform_name(self) -> str:
        return self.task_id

    def complete_params(self) -> Dict[str, Any]:
        return dict(task_id=self.task_id,
                    download_date=self.source.download_date)

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self._make_url(self.account),
                            self.project.upload_table,
                            self.transform_name, self.source,
                            echo=self.echo)

    def requires(self) -> List[luigi.Task]:
        return [self.project, self.source] + SqlScriptTask.requires(self)

    def complete(self) -> bool:
        # Belt and suspenders
        return (self.output().exists() and
                SqlScriptTask.complete(self))

    @property
    def label(self) -> str:
        return self.script.title

    def run(self) -> None:
        upload = self._upload_target()
        with upload.job(self,
                        label=self.label,
                        user_id=make_url(self.account).username) as conn_id_r:
            conn, upload_id, result = conn_id_r
            bulk_rows = SqlScriptTask.run_event(
                self, conn,
                run_vars=dict(upload_id=str(upload_id)),
                script_params=dict(self.script_params(), upload_id=upload_id))
            result[upload.table.c.loaded_record.name] = bulk_rows

    def script_params(self) -> Params:
        return dict(download_date=self.source.download_date,
                    project_id=self.project.project_id)

    def is_bulk(self, statement: SQL) -> bool:
        return insert_append_table(statement) is not None

    def bulk_insert(self, conn: LoggedConnection, fname: str, line: int,
                    statement: SQL, run_params: Params,
                    bulk_rows: int) -> int:
        with conn.log.step(
                '%(filename)s:%(lineno)s: %(event)s',
                dict(event='bulk_insert',
                     filename=fname, lineno=line)) as step:
            plan = '\n'.join(explain_plan(conn, statement))
            conn.log.info('%(filename)s:%(lineno)s: %(event)s:\n%(plan)s',
                          dict(filename=fname, lineno=line, event='plan',
                               plan=plan))

            params = params_used(run_params, statement)
            self.set_status_message(
                '%s:%s:\n%s\n%s' % (fname, line, statement, params))
            last_result = conn.execute(statement, params)
            step.msg_parts.append(' %(rowcount)d rows')
            step.argobj.update(dict(rowcount=last_result.rowcount))
            bulk_rows += last_result.rowcount

        return bulk_rows


class UploadTarget(DBTarget):
    def __init__(self, connection_string: str,
                 table: sqla.Table, transform_name: str, source: SourceTask,
                 echo: bool=False) -> None:
        DBTarget.__init__(self, connection_string,
                          echo=echo)
        self.table = table
        self.source = source
        self.transform_name = transform_name
        self.upload_id = None  # type: Opt[int]

    def __repr__(self) -> str:
        return '%s(transform_name=%s)' % (
            self.__class__.__name__, self.transform_name)

    def exists(self) -> bool:
        conn = ConnectionProblem.tryConnect(self.engine)
        with conn.begin():
            up_t = self.table
            upload_id = conn.scalar(
                sqla.select([sqla.func.max(up_t.c.upload_id)])
                .select_from(up_t)
                .where(sqla.and_(up_t.c.transform_name == self.transform_name,
                                 up_t.c.load_status == 'OK')))
            return upload_id is not None

    @contextmanager
    def job(self, task: DBAccessTask,
            label: Opt[str] = None, user_id: Opt[str] = None,
            upload_id: Opt[int] = None) -> Iterator[
            Tuple[LoggedConnection, int, Params]]:
        event = 'upload job'
        with task.connection(event=event) as conn:
            up_t = self.table
            if upload_id is None:
                if user_id is None:
                    raise TypeError('must supply user_id for new record')
                if label is None:
                    raise TypeError('must supply label for new record')
                upload_id = self.insert(conn, label, user_id)
            else:
                [label, user_id] = conn.execute(
                    sqla.select([up_t.c.upload_label, up_t.c.user_id])
                    .where(up_t.c.upload_id == upload_id)).fetchone()

            msg = ' %(upload_id)s for %(label)s'
            info = dict(label=label, upload_id=upload_id)
            conn.step.msg_parts.append(msg)
            conn.step.argobj.update(info)
            conn.log.info(msg, info)  # Go ahead and log the upload_id early.

            result = {}  # type: Params
            yield conn, upload_id, result
            conn.execute(up_t.update()
                         .where(up_t.c.upload_id == upload_id)
                         .values(load_status='OK', end_date=func.now(),
                                 **result))

    def insert(self, conn: LoggedConnection, label: str, user_id: str) -> int:
        '''
        :param label: a label for related facts for audit purposes
        :param user_id: an indication of who uploaded the related facts
        '''
        up_t = self.table
        next_q = sql_text(
            '''select {i2b2}.sq_uploadstatus_uploadid.nextval
            from dual'''.format(i2b2=self.table.schema))
        upload_id = conn.scalar(next_q)  # type: int

        conn.execute(up_t.insert()
                     .values(upload_id=upload_id,
                             upload_label=label,
                             user_id=user_id,
                             source_cd=self.source.source_cd,
                             load_date=sqla.func.now(),
                             transform_name=self.transform_name))
        self.upload_id = upload_id
        return upload_id


class I2B2ProjectCreate(DBAccessTask):
    star_schema = StrParam(description='see client.cfg')
    project_id = StrParam(description='see client.cfg')
    _meta = None          # type: Opt[sqla.MetaData]
    _upload_table = None  # type: Opt[sqla.Table]

    Column, ty = sqla.Column, sqla.types
    upload_status_columns = [
        Column('upload_id', ty.Numeric(38, 0, asdecimal=False), primary_key=True),
        Column('upload_label', ty.String(500), nullable=False),
        Column('user_id', ty.String(100), nullable=False),
        Column('source_cd', ty.String(50), nullable=False),
        Column('no_of_record', ty.Numeric(asdecimal=False)),
        Column('loaded_record', ty.Numeric(asdecimal=False)),
        Column('deleted_record', ty.Numeric(asdecimal=False)),
        Column('load_date', ty.DateTime, nullable=False),
        Column('end_date', ty.DateTime),
        Column('load_status', ty.String(100)),
        Column('message', ty.Text),
        Column('input_file_name', ty.Text),
        Column('log_file_name', ty.Text),
        Column('transform_name', ty.String(500)),
    ]

    def output(self) -> 'SchemaTarget':
        return SchemaTarget(self._make_url(self.account),
                            schema_name=self.star_schema,
                            table_eg='patient_dimension',
                            echo=self.echo)

    def run(self) -> None:
        raise NotImplementedError('see heron_create.create_deid_datamart etc.')

    @property
    def metadata(self) -> sqla.MetaData:
        if self._meta:
            return self._meta
        self._meta = meta = sqla.MetaData(schema=self.star_schema)
        return meta

    def table_details(self, lc: LoggedConnection, tables: List[str]) -> sqla.MetaData:
        i2b2_meta = sqla.MetaData(schema=self.star_schema)
        i2b2_meta.reflect(only=tables, schema=self.star_schema,
                          bind=self._dbtarget().engine)
        return i2b2_meta

    @property
    def upload_table(self) -> sqla.Table:
        if self._upload_table is not None:
            return self._upload_table
        t = sqla.Table(
            'upload_status', self.metadata,
            *self.upload_status_columns,
            schema=self.star_schema)
        self._upload_table = t
        return t


class SchemaTarget(DBTarget):
    def __init__(self, connection_string: str, schema_name: str, table_eg: str,
                 echo: bool=False) -> None:
        DBTarget.__init__(self, connection_string, echo=echo)
        self.schema_name = schema_name
        self.table_eg = table_eg

    def exists(self) -> bool:
        table = Table(self.table_eg, sqla.MetaData(), schema=self.schema_name)
        return table.exists(bind=self.engine)  # type: ignore


class ConnectionProblem(DatabaseError):
    '''Provide hints about ssh tunnels.
    '''
    # connection closed, no listener
    tunnel_hint_codes = [12537, 12541]

    @classmethod
    def tryConnect(cls, engine: Engine) -> Connection:
        try:
            return engine.connect()
        except DatabaseError as exc:
            raise ConnectionProblem.refine(exc, str(engine)) from None

    @classmethod
    def refine(cls, exc: Exception, conn_label: str) -> Exception:
        '''Recognize known connection problems.

        :returns: customized exception for known
                  problem else exc
        '''
        ora_ex = maybe_ora_err(exc)

        if ora_ex:
            return cls(exc, ora_ex, conn_label)
        return exc

    def __init__(self, exc: DatabaseError, ora_ex: Ora_Error, conn_label: str) -> None:
        DatabaseError.__init__(
            self,
            exc.statement, exc.params,
            exc.connection_invalidated)
        message = '%s <%s>'
        args = [ora_ex, conn_label]

        if exc.statement and ora_ex.offset:
            stmt_rest = exc.statement[
                ora_ex.offset:ora_ex.offset + 120]
            message += '\nat: %s'
            args += [stmt_rest]
        local_conn_prob = (
            ora_ex.code in self.tunnel_hint_codes and
            'localhost' in conn_label)
        if local_conn_prob:
            message += '\nhint: ssh tunnel down?'
        message += '\nin: %s'
        args += [ora_ex.context]
        self.message = message
        self.args = tuple(args)

    def __str__(self) -> str:
        return self.message % self.args


class ReportTask(DBAccessTask):
    @property
    def script(self) -> Script:
        raise NotImplementedError('subclass must implement')

    @property
    def report_name(self) -> str:
        raise NotImplementedError('subclass must implement')

    def complete(self) -> bool:
        '''Double-check requirements as well as output.
        '''
        deps = luigi.task.flatten(self.requires())  # type: List[luigi.Task]
        return (self.output().exists() and
                all(t.complete() for t in deps))

    def _csvout(self) -> 'CSVTarget':
        return CSVTarget(path=self.report_name + '.csv')

    def output(self) -> luigi.Target:
        return self._csvout()

    def run(self) -> None:
        with self.connection('report') as conn:
            query = sql_text(
                'select * from {object}'.format(object=self.report_name))
            result = conn.execute(query)
            cols = result.keys()
            rows = result.fetchall()
            self._csvout().export(cols, rows)


class CSVTarget(luigi.local_target.LocalTarget):
    def export(self, cols: List[str], data: List) -> None:
        with self.open('wb') as stream:
            dest = csv.writer(stream)
            dest.writerow(cols)
            dest.writerows(data)

    @contextmanager
    def dictreader(self,
                   lowercase_fieldnames: bool=False,
                   delimiter: str=',') -> Iterator[csv.DictReader]:
        '''DictReader contextmanager

        @param lowercase_fieldnames: sqlalchemy uses lower-case bind
               parameter names, but SCILHS CSV file headers use the
               actual uppercase column names. So we got:

                 CompileError: The 'oracle' dialect with current
                 database version settings does not support empty
                 inserts.


        '''
        with self.open('rb') as stream:
            dr = csv.DictReader(stream, delimiter=delimiter)
            if lowercase_fieldnames:
                # This is a bit of a kludge, but it works...
                dr.fieldnames = [n.lower() for n in dr.fieldnames]
            yield dr


class AdHoc(DBAccessTask):
    sql = StrParam()
    name = StrParam()

    def _csvout(self) -> CSVTarget:
        return CSVTarget(path=self.name + '.csv')

    def output(self) -> luigi.Target:
        return self._csvout()

    def run(self) -> None:
        with self.connection() as work:
            result = work.execute(self.sql)
            cols = result.keys()
            rows = result.fetchall()
            self._csvout().export(cols, rows)


class KillSessions(DBAccessTask):
    reason = StrParam(default='*no reason given*')
    sql = '''
    begin
      sys.kill_own_sessions(:reason);
    end;
    '''

    def complete(self) -> bool:
        return False

    def run(self) -> None:
        with self.connection('kill own sessions') as work:
            work.execute(self.sql, params=dict(reason=self.reason))


class AlterStarNoLogging(DBAccessTask):
    sql = '''
    alter table TABLE nologging
    '''
    tables = ['patient_mapping',
              'encounter_mapping',
              'patient_dimension',
              'visit_dimension',
              'observation_fact']

    def complete(self) -> bool:
        return False

    def run(self) -> None:
        with self.connection() as work:
            for table in self.tables:
                work.execute(self.sql.replace('TABLE', table))


class MigrateUpload(SqlScriptTask, I2B2Task):
    upload_id = IntParam()
    workspace_star = StrParam()
    parallel_degree = IntParam(default=24,
                               significant=False)

    script = Script.pcornet_loader

    @property
    def variables(self) -> Environment:
        return dict(I2B2STAR=self.project.star_schema,
                    workspace_star=self.workspace_star,
                    parallel_degree=str(self.parallel_degree),
                    upload_id=str(self.upload_id))


class MigratePendingUploads(DBAccessTask, I2B2Task, luigi.WrapperTask):
    workspace_star = StrParam()

    find_pending = """
    select upload_id from %(WORKSPACE)s.upload_status
    where load_status in ('OK', 'OK_work') and upload_id not in (
      select upload_id from %(I2B2STAR)s.upload_status
      where load_status='OK' )
    """

    def requires(self) -> List[luigi.Task]:
        find_pending = self.find_pending % dict(
            WORKSPACE=self.workspace_star,
            I2B2STAR=self.project.star_schema)

        deps = []  # type: List[luigi.Task]
        with self.connection('pending uploads') as lc:
            pending = [row.upload_id for row in
                       lc.execute(find_pending).fetchall()]

            workmeta = sqla.MetaData()
            for upload_id in pending:
                table = Table('observation_fact_%d' % upload_id, workmeta,
                              schema=self.workspace_star)
                if table.exists(bind=lc._conn):
                    deps.append(
                        MigrateUpload(upload_id=upload_id,
                                      workspace_star=self.workspace_star))
                else:
                    log.warn('no such table to migrate: %s', table)
        return deps
