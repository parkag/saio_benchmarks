#!/usr/bin/python

"""
GEQO seems to win in this case
"""

def get_schema_query(n):
    sql = """SET SEARCH_PATH = test_data, test_view;\n
    \rDROP SCHEMA IF EXISTS test_view CASCADE;\n"""

    sql += 'DROP TABLE IF EXISTS center;\n'
    sql += 'CREATE TABLE center ('
    sql += ', '.join(("col%d int" % i for i in range(n)))
    sql += ');\n'

    for i in xrange(n):
        sql += 'DROP TABLE IF EXISTS arm%d;' % i
        sql += 'CREATE TABLE arm%d (col int);\n' % i
    
    sql += '\n'

    for i in xrange(n):
        sql += "INSERT INTO arm%d (select generate_series(1, %d));\n" % (i, 10000*(i + 1))
    
    sql += 'INSERT INTO center('
    sql += ', '.join(("col%d" % i for i in range(n)))
    sql += ') VALUES ('
    sql += ', '.join(("%d" % i for i in range(n)))
    sql += ');\n'

    print sql
    return sql


def get_bigger_schema_query(n):
    sql = 'SET SEARCH_PATH = test_data, test_view;\n'
    sql += 'DROP SCHEMA IF EXISTS test_view CASCADE;\n'

    sql += 'DROP TABLE IF EXISTS center;\n'
    sql += 'create table center ('
    sql += ', '.join(("col%d int" % i for i in range(n)))
    sql += ');\n'

    sql += '\n'.join(('drop table if exists arm%d; create table arm%d (col '
                      'int, col text);' % (i, i) for i in range(n)))
    sql += '\n'

    sql += '\n'.join(("insert into arm%d (select generate_series(1, %d), 'smth');"
                      % (i, 10000*(i+1)) for i in range(n)))
    sql += 'insert into center('
    sql += ', '.join(("col%d" % i for i in range(n)))
    sql += ') values ('
    sql += ', '.join(("%d" % i for i in range(n)))
    sql += ');\n'

    print sql
    return sql

def get_analyze_query(n):
    # for the provided dataset every solution is equivalent
    sql = 'SET SEARCH_PATH = test_data, test_view;\n'
    sql += 'analyze;\n'

    sql += 'explain (FORMAT JSON) select * from '
    sql += ', '.join(("arm%d" % i for i in range(n / 2)))
    sql += ', center, '
    sql += ', '.join(("arm%d" % i for i in range(n / 2, n)))
    sql += ' where '
    sql += ' and '.join(('arm%d.col = col%d ' % (i, i) for i in range(n)))
    sql += ';'

    print sql
    return sql


def get_analyze_query2(n):
    sql = 'SET SEARCH_PATH = test_data, test_view;\n'
    sql += 'analyze;\n'

    sql += 'explain (FORMAT JSON) select * from '
    sql += ', '.join(("arm%d" % i for i in range(n / 2)))
    sql += ', center, '
    sql += ', '.join(("arm%d" % i for i in range(n / 2, n)))
    sql += ' where '
    sql += ' and '.join(('(arm%d.col = col%d or arm%d.col = col%d)' % (i, i, i, i%5+i%3) for i in range(n)))
    sql += ';'

    print sql
    return sql


def get_analyze_query2_join(n):
    sql = 'SET SEARCH_PATH = test_data, test_view;\n'
    sql += 'analyze;\n'

    sql += 'explain (FORMAT JSON) SELECT * FROM center'

    for i in xrange(n):
        sql += " JOIN arm%d " % i
        sql += 'ON (arm%d.col = center.col%d or arm%d.col = center.col%d)\n' % (i, i, i, i%5+i%3)
    
    sql += ';'

    print sql
    return sql


def get_analyze_query2_left_join(n):
    sql = 'SET SEARCH_PATH = test_data, test_view;\n'
    sql += 'analyze;\n'

    sql += 'explain (FORMAT JSON) select * from '
    sql += ', '.join(("arm%d" % i for i in range(n / 2)))
    sql += ', center, '
    sql += ', '.join(("arm%d" % i for i in range(n / 2, n)))
    sql += ' where '
    sql += ' and '.join(('(arm%d.col = col%d or arm%d.col = col%d)' % (i, i, i, i%5+i%3) for i in range(n)))
    sql += ';'

    print sql
    return sql


if __name__ == "__main__":
    import sys
    print star(int(sys.argv[1]))
