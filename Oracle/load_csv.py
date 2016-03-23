''' load_csv.py - Load a specified data file with sqlplus/sqlldr
'''
from collections import defaultdict
from contextlib import contextmanager
from csv import DictReader
from subprocess import PIPE
import logging

# OCAP exception for logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y.%m.%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)


def main(argv, environ, open_argv, mk_tool):
    # TODO: Consider docopt
    table_name, csv, ctl_out_fn, user_env, password_env = argv[1:6]

    table_tool = mk_tool(table_name=table_name,
                         user=environ[user_env],
                         password=environ[password_env])

    with open_argv(csv, 'rb') as fin:
        dr = DictReader(fin)
        table_tool.create(dr=dr)

    with open_argv(ctl_out_fn, 'wb') as ctl_fout:
        ctl_code = table_tool.ctl_from_csv(fields=dr.fieldnames)
        ctl_fout.write(ctl_code)

    table_tool.load(ctl_fn=ctl_out_fn, csv=csv)


class MockPopen(object):
    def __init__(self, cmd_args, stdout=None, stderr=None, shell=False):
        self._cmd_args = cmd_args
        self.returncode = 0

    def communicate(self):
        log.info('MockPopen::communicate: %s' % (self._cmd_args))
        return ('', '')


class TableTool(object):
    '''
    Test DDL generation - note test for 0-length column
    >>> from StringIO import StringIO
    >>> sio = StringIO('\\n'.join([l.strip() for l in """col1,col2,col3
    ...           some data,Some other data and stuff,""".split('\\n')]))
    >>> s = TableTool('me', 'sekret', 'some_table', MockPopen)
    >>> print s.ddl_from_csv(DictReader(sio))
    ... # doctest: +NORMALIZE_WHITESPACE
    create table some_table (
        col1 varchar2(16),
        col2 varchar2(32),
        col3 varchar2(16)
        );

    Test control file generation
    >>> print s.ctl_from_csv(['col1', 'col2'])
    ... # doctest: +NORMALIZE_WHITESPACE
        options (errors=0, skip=1)
        load data
        truncate into table some_table
        fields terminated by ',' optionally enclosed by '"'
        trailing nullcols(
        col1,
        col2
        )
    '''
    def __init__(self, Popen, user, password, table_name,
                 sqlplus_cmd='sqlplus /nolog',
                 sqlldr_cmd='sqlldr'):
        def run(template):
            cmd = dedent(template.format(
                sqlplus=sqlplus_cmd, sqlldr=sqlldr_cmd,
                user=user, password=password,
                table_name=table_name))
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = p.communicate()
            if stdout:
                log.info('stdout: %s', stdout)
            if stderr:
                log.warn('stderr: %s', stderr)
            if p.returncode:
                raise IOError(p.returncode)

        self.run = run
        self.table_name = table_name

    @classmethod
    def make(cls, Popen, user, password, name):
        return TableTool(Popen, user, password, name)

    def create(self, dr):
        # Note: shell = true might not be the most secure, but it makes
        # piplining easier.
        # See also https://docs.python.org/2.7/library/subprocess.html#replacing-shell-pipeline  # noqa
        # Also, be warned that SQL injection is possible with arguments!
        cmd = ("""{sqlplus} <<EOF
        connect {user}/{password};
        set echo on;
        WHENEVER SQLERROR CONTINUE;
        drop table {table_name};
        WHENEVER SQLERROR EXIT SQL.SQLCODE;
        %(create)s
        EOF""") % dict(create=self.ddl_from_csv(dr))
        self.run(cmd)

    def load(self, ctl_fn, csv):
        cmd = ('{sqlldr} {user}/{password} control=%(ctl_fn)s '
               'data=%(csv)s log=%(log)s bad=%(bad)s '
               'errors=0'
               % dict(ctl_fn=ctl_fn, csv=csv,
                      log=csv + '.log', bad=csv + '.bad'))
        self.run(cmd)

    def ddl_from_csv(self, dr):
        def sz(l, chunk=16):
            return max(chunk, chunk * ((l + chunk - 1) / chunk))

        mcl = defaultdict(int)
        for row in dr:
            # Use fieldnames to maintain column ordering
            for col in dr.fieldnames:
                mcl[col] = sz(max(mcl[col], len(row[col])))
        return ('create table %s (\n  ' % self.table_name +
                ',\n  '.join(['%s varchar2(%s)' % (c, mcl[c])
                              for c in dr.fieldnames]) + '\n);')

    def ctl_from_csv(self, fields, delim=','):
        ctl = ('''options (errors=0, skip=1)
               load data
               truncate into table %(schema_table)s
               fields terminated by '%(delim)s' optionally enclosed by '"'
               trailing nullcols(
               %(columns)s
           )''') % dict(schema_table=self.table_name,
                        columns=', \n'.join(fields),
                        delim=delim)
        return dedent(ctl)


def dedent(txt):
    '''spaces from indented triple-quoted strings are kinda annoying.
    '''
    return '\n'.join(line.strip()
                     for line in txt.split('\n'))


if __name__ == '__main__':
    def _tcb():
        from os import environ
        from subprocess import Popen
        from sys import argv

        @contextmanager
        def open_argv(fn, mode):
            if fn not in argv[1:]:
                raise KeyError('Attempt to open file not on command line!')
            with open(fn, mode) as f:
                yield f

        def mk_tool(table_name, user, password):
            return TableTool.make(
                MockPopen if '--dry-run' in argv else Popen,
                user, password, table_name)

        main(argv=argv, environ=environ, open_argv=open_argv,
             mk_tool=mk_tool)

    _tcb()
