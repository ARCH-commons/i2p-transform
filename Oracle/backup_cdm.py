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
        if has_rows(cursor, table):
            drop(cursor, table, backup_schema)
            copy(cursor, table, table, backup_schema)


def drop(cursor, table, backup_schema):
    try:
        cursor.execute('drop table %(backup_schema)s.%(table)s' %
                       dict(backup_schema=backup_schema, table=table))
    except DatabaseError, e:  # noqa
        # Table might not exist
        pass


def has_rows(cursor, table):
    ret = False
    try:
        cursor.execute('select count(*) from %(table)s' %
                       dict(table=table))
        if int(cursor.fetchall()[0][0]) > 0:
            ret = True
    except:
        pass
    return ret


def copy(cursor, table, new_name, backup_schema):
    cursor.execute('create table %(backup_schema)s.%(table)s as '
                   'select * from %(table)s' %
                   dict(backup_schema=backup_schema, table=table))


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
