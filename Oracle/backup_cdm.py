''' backup_cdm - back up cdm tables that have at least one row
'''
from __future__ import print_function

import csv
import pkg_resources as pkg
from sys import stderr

# Allow --dry run even without cx_Oracle
try:
    from cx_Oracle import DatabaseError
except ImportError:
    pass

CDM_SPEC = '../2015-06-01-PCORnet-Common-Data-Model-v3dot0-parseable-fields.csv'  # noqa
KUMC_ADDON_SPEC = 'backup_kumc_specific_tables.csv'

CDM3_TABLES = set(f['TABLE_NAME']
                  for f in csv.DictReader(
                                 pkg.resource_stream(__name__, CDM_SPEC)))

KUMC_ADDON_TABLES = set(f['TABLE_NAME']
                        for f in csv.DictReader(
                                 pkg.resource_stream(__name__, KUMC_ADDON_SPEC)))

CDM3_TABLES = sorted(CDM3_TABLES.union(KUMC_ADDON_TABLES))


def main(get_cursor):
    cursor, backup_schema = get_cursor()

    for table in CDM3_TABLES:
        bak_schema_table = ('%(backup_schema)s.%(table)s' %
                            dict(backup_schema=backup_schema,
                                 table=table))
        eprint('%(table)s (to be backed up) currently has %(rcount)s rows.'
               % dict(table=table, rcount=count_rows(cursor, table)))

        drop(cursor, bak_schema_table)
        copy(cursor, table, bak_schema_table)
        eprint('%(bak_schema_table)s now has %(rows)s rows.' %
               dict(bak_schema_table=bak_schema_table,
                    rows=count_rows(cursor, bak_schema_table)))


def eprint(*args, **kwargs):
    ''' ACK: http://stackoverflow.com/questions/5574702/how-to-print-to-stderr-in-python  # noqa
    '''
    print(*args, file=stderr, **kwargs)


def drop(cursor, schema_table):
    eprint('Dropping %(schema_table)s (%(rows)s rows).' %
           dict(schema_table=schema_table,
                rows=count_rows(cursor, schema_table)))
    try:
        cursor.execute('drop table %(schema_table)s' %
                       dict(schema_table=schema_table))
    except DatabaseError, e:  # noqa
        # Table might not exist
        pass


def count_rows(cursor, schema_table):
    ret = None
    try:
        cursor.execute('select count(*) from %(schema_table)s' %
                       dict(schema_table=schema_table))
        ret = int(cursor.fetchall()[0][0])
    except:  # noqa
        pass
    return ret


def copy(cursor, table, bak_schema_table):
    eprint('Copying %(table)s to %(bak_schema_table)s.' %
           dict(table=table, bak_schema_table=bak_schema_table))
    cursor.execute('create table %(bak_schema_table)s as '
                   'select * from %(table)s' %
                   dict(bak_schema_table=bak_schema_table, table=table))


class MockCursor(object):
    def __init__(self):
        self.fetch_count = 1

    def execute(self, sql):
        eprint('execute: ' + sql)

    def fetchall(self):
        r = [(self.fetch_count, )]
        self.fetch_count += 1
        eprint('fetch returning: ' + str(r))
        return r


if __name__ == '__main__':
    def _tcb():
        from os import environ
        from sys import argv

        def get_cursor():
            host, port, sid, backup_schema = argv[1:5]
            user = environ['pcornet_cdm_user']
            password = environ['pcornet_cdm']

            if '--dry-run' in argv:
                return MockCursor(), backup_schema
            else:
                import cx_Oracle as cx
                conn = cx.connect(user, password,
                                  dsn=cx.makedsn(host, port, sid))
                return conn.cursor(), backup_schema

        main(get_cursor)
    _tcb()
