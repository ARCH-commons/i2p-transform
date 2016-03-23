''' load_csv.py - Load a specified data file with sqlplus/sqlldr
'''
from contextlib import contextmanager
from collections import defaultdict
from csv import DictReader
from functools import partial
import logging

# OCAP exception for logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y.%m.%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)


def main(argv, environ, open_argv, mk_sqlplus):
    def logif(logf, s):
        if s:
            logf(s)

    sp = mk_sqlplus()

    # TODO: Consider docopt
    table_name, csv, ctl_out_fn, user_env, password_env = argv[1:6]

    # Use Oracle utilities to drop/create table based on csv input
    with open_argv(csv, 'rb') as fin:
        dr = DictReader(fin)
        ret, so, se = sp.create_table(user=environ[user_env],
                                      password=environ[password_env],
                                      table_name=table_name,
                                      dr=dr)
    logif(log.info, so)
    logif(log.error, se)
    if ret:
        raise RuntimeError()

    # Write out a control file based on csv input
    with open_argv(ctl_out_fn, 'wb') as ctl_fout:
        ctl_fout.write(sp.ctl_from_csv(schema_table=table_name,
                                       fields=dr.fieldnames))

    # Load .csv using Oracle utilities
    ret, so, se = sp.load_table(user=environ[user_env],
                                password=environ[password_env],
                                table_name=table_name,
                                ctl_fn=ctl_out_fn,
                                csv=csv)
    logif(log.info, so)
    logif(log.error, se)
    if ret:
        raise RuntimeError()


class MockPopen(object):
    def __init__(self, cmd_args, stdout=None, stderr=None, shell=False):
        self ._cmd_args = cmd_args

    def communicate(self):
        log.info('MockPopen::communicate: %s' % (self._cmd_args))
        return ('', '')


class OracleUtils(object):
    '''
    Test DDL generation - note test for 0-length column
    >>> from StringIO import StringIO
    >>> sio = StringIO('\\n'.join([l.strip() for l in """col1,col2,col3
    ...           some data,Some other data and stuff,""".split('\\n')]))
    >>> s = OracleUtils(MockPopen, None)
    >>> print s.ddl_from_csv('some_table', DictReader(sio))
    ... # doctest: +NORMALIZE_WHITESPACE
    create table some_table (
        col1 varchar2(16),
        col2 varchar2(32),
        col3 varchar2(16)
        );

    Test control file generation
    >>> print s.ctl_from_csv('some_table', ['col1', 'col2'])
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
    def __init__(self, Popen, PIPE, sqlplus_cmd='sqlplus /nolog',
                 sqlldr_cmd='sqlldr'):
        self._sqlplus_cmd = sqlplus_cmd
        self._sqlldr_cmd = sqlldr_cmd
        self._Popen = Popen
        self.PIPE = PIPE

    @classmethod
    def make(cls, Popen, PIPE):
        return OracleUtils(Popen, PIPE)

    def create_table(self, user, password, table_name, dr):
        # Note: shell = true might not be the most secure, but it makes
        # piplining easier.
        # See also https://docs.python.org/2.7/library/subprocess.html#replacing-shell-pipeline  # noqa
        # Also, be warned taht SQL injection is possible with arguments!
        cmd = self._sqlplus_cmd + ' ' + (
            '\n'.join([c.strip() for c in """<<EOF
                       connect %(user)s/%(password)s;
                       set echo on;
                       WHENEVER SQLERROR CONTINUE;
                       drop table %(table_name)s;
                       WHENEVER SQLERROR EXIT SQL.SQLCODE;
                       %(create)s
                       EOF""".split('\n')])
            % dict(user=user, password=password, table_name=table_name,
                   create=self.ddl_from_csv(table_name, dr)))
        p = self._Popen(cmd, stdout=self.PIPE, stderr=self.PIPE, shell=True)
        so, se = p.communicate()
        return p.returncode, so, se

    def load_table(self, user, password, table_name, ctl_fn, csv):
        cmd = self._sqlldr_cmd + (' %(user)s/%(password)s control=%(ctl_fn)s '
                                  'data=%(csv)s log=%(log)s bad=%(bad)s '
                                  'errors=0'
                                  % dict(user=user, password=password,
                                         ctl_fn=ctl_fn, csv=csv,
                                         log=csv + '.log', bad=csv + '.bad'))
        p = self._Popen(cmd, stdout=self.PIPE, stderr=self.PIPE, shell=True)
        so, se = p.communicate()
        return p.returncode, so, se

    @staticmethod
    def ddl_from_csv(table_name, dr):
        def sz(l, chunk=16):
            return max(chunk, chunk * ((l+chunk-1)/chunk))

        mcl = defaultdict(int)
        for row in dr:
            # Use fieldnames to maintain column ordering
            for col in dr.fieldnames:
                mcl[col] = sz(max(mcl[col], len(row[col])))
        return ('create table %s (\n  ' % table_name +
                ',\n  '.join(['%s varchar2(%s)' % (c, mcl[c])
                              for c in dr.fieldnames]) + '\n);')

    @staticmethod
    def ctl_from_csv(schema_table, fields, delim=','):
        ctl = ('''options (errors=0, skip=1)
               load data
               truncate into table %(schema_table)s
               fields terminated by '%(delim)s' optionally enclosed by '"'
               trailing nullcols(
               %(columns)s
           )''') % dict(schema_table=schema_table,
                        columns=', \n'.join(fields),
                        delim=delim)
        return '\n'.join([l.strip() for l in ctl.split('\n')])


if __name__ == '__main__':
    def _tcb():
        from os import environ
        from subprocess import Popen, PIPE
        from sys import argv

        @contextmanager
        def open_argv(fn, mode):
            if fn not in argv[1:]:
                raise KeyError('Attempt to open file not on command line!')
            with open(fn, mode) as f:
                yield f

        return(dict(argv=argv, environ=environ, open_argv=open_argv,
                    mk_sqlplus=partial(
                        OracleUtils.make,
                        Popen=MockPopen if '--dry-run' in argv else Popen,
                        PIPE=PIPE)))

    main(**_tcb())