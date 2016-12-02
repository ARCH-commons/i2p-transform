''' backup_cdm - back up cdm tables that have at least one row
'''
# Allow --dry run even without cx_Oracle
try:
    from cx_Oracle import DatabaseError
except:
    pass

CDM3_TABLES = ['HARVEST', 'DEMOGRAPHIC', 'ENCOUNTER', 'DIAGNOSIS',
               'CONDITION', 'PROCEDURES', 'VITAL', 'ENROLLMENT',
               'LAB_RESULT_CM', 'PRESCRIBING', 'DISPENSING']


def main(get_cursor):
    cursor, backup_schema = get_cursor()

    for table in CDM3_TABLES:
        bak_schema_table = ('%(backup_schema)s.%(table)s' %
                            dict(backup_schema=backup_schema,
                                 table=table))
        rcount = count_rows(cursor, table)
        print ('%(table)s (to be backed up) currently has %(rcount)s rows.'
               % dict(table=table, rcount=rcount))

        if rcount:
            drop(cursor, bak_schema_table)
            copy(cursor, table, bak_schema_table)
            print ('%(bak_schema_table)s now has %(rows)s rows.' %
                   dict(bak_schema_table=bak_schema_table,
                        rows=count_rows(cursor, bak_schema_table)))


def drop(cursor, schema_table):
    print ('Dropping %(schema_table)s (%(rows)s rows).' %
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
    except:
        pass
    return ret


def copy(cursor, table, bak_schema_table):
    print ('Copying %(table)s to %(bak_schema_table)s.' %
           dict(table=table, bak_schema_table=bak_schema_table))
    cursor.execute('create table %(bak_schema_table)s as '
                   'select * from %(table)s' %
                   dict(bak_schema_table=bak_schema_table, table=table))


class MockCursor(object):
    def __init__(self):
        self.fetch_count = 1

    def execute(self, sql):
        print 'execute: ' + sql

    def fetchall(self):
        r = [(self.fetch_count, )]
        self.fetch_count += 1
        print 'fetch returning: ' + str(r)
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
