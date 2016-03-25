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
    cursor = get_cursor()

    for table in CDM3_TABLES:
        if has_rows(cursor, table):
            drop_triggers(cursor, table)
            drop(cursor, table + '_BAK')
            rename(cursor, table, table + '_BAK')


def drop_triggers(cursor, table):
    cursor.execute("""select trigger_name
                   from user_triggers
                   where table_name = '%(table)s'""" %
                   dict(table=table))

    for res in cursor.fetchall():
        cursor.execute('drop trigger %(trigger)s' % dict(trigger=res[0]))


def drop(cursor, table):
    try:
        cursor.execute('drop table %(table)s' % dict(table=table))
    except DatabaseError, e:
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


def rename(cursor, table, new_name):
    cursor.execute('alter table %(table)s rename to %(new_name)s' %
                   dict(table=table, new_name=new_name))


class MockCursor(object):
    def __init__(self):
        self.fetch_count = 0

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
            host, port, sid = argv[1:4]
            user = environ['pcornet_cdm_user']
            password = environ['pcornet_cdm']

            if '--dry-run' in argv:
                return MockCursor()
            else:
                import cx_Oracle as cx
                conn = cx.connect(user, password,
                                  dsn=cx.makedsn(host, port, sid))
                return conn.cursor()

        main(get_cursor)
    _tcb()
