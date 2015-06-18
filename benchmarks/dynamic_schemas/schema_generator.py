import random
from data_generator import *

CONSTRAINTS = ['NOT NULL', 'UNIQUE', 'PRIMARY KEY']


class RandomSchema(object):

    def __init__(self, ntables, nindexes, datatypes=['text']):
        self.ntables = ntables
        self.nindexes = nindexes
        self.tables = []
        self.indexes = []
        self.indexed_tables = []
        self.datatypes = datatypes

    def generate_tables(self, min_cols=3, max_cols=12):
        for i in xrange(self.ntables):
            table_name = "table_{num}".format(num=i)
            ncols = random.randint(min_cols, max_cols)
            table = BaseRandomTable(
                table_name,
                ncols=ncols, #datatypes=standard_datatypes)
                datatypes=self.datatypes)
            # TODO: dynamic column distribution
            table.generate_columns()
            self.tables.append(table)

    def generate_indexes(self):
        for i in xrange(self.nindexes):
            tab = random.choice(self.tables)
            while tab in self.indexed_tables:
                tab = random.choice(self.tables)

            column = random.choice(tab.columns)
            
            name = 'index_'+str(random.randint(0,100))+'_'+str(random.randint(0,100))
            index = Index(name, tab, column)
            
            self.indexed_tables.append(tab)
            self.indexes.append(index)

    def sql(self):
        sql = '--\n'
        sql += """DROP SCHEMA IF EXISTS test_view CASCADE;\n
            CREATE SCHEMA test_view;\n
            SET SEARCH_PATH = test_view, test_data;\n"""
        for table in self.tables:
            sql += table.sql()

        for index in self.indexes:
            sql += index.sql()

        # dropping it here so that it will be called only once during setup
        sql += "ANALYZE;"
        return sql


class BaseRandomTable(object):

    def __init__(self, table_name, ncols, datatypes):
        self.name = table_name
        self.ncols = ncols
        self.indexes = []
        self.datatypes = datatypes
        self.columns = []

    def generate_columns(self):
        for i in xrange(self.ncols):
            datatype = random.choice(self.datatypes)
            column_name = "col_{num}".format(num=i)
            column = Column(column_name, datatype)
            self.columns.append(column)

    def sql(self):
        sql = "DROP TABLE IF EXISTS {table_name};\n".format(
            table_name=self.name)
        sql += """CREATE TABLE {table_name}(
                \r{columns_section}
            \r);
        """.format(
            table_name=self.name,
            columns_section=",\n".join([col.sql() for col in self.columns])
        )
        return sql


class Index(object):

    def __init__(self, name, table, column):
        self.name = name
        self.table = table
        self.column = column

    def sql(self):
        sql = "CREATE INDEX ON {table_name} ({column_name});\n".format(
            table_name=self.table.name,
            column_name=self.column.name
        )
        return sql



class Column(object):
    
    def __init__(self, name, datatype, constraints=[]):
        self.name = name
        self.datatype = datatype
        self.constraints = constraints

    def sql(self):
        sql = "{name} {datatype}".format(
            name=self.name,
            datatype=self.datatype,
        )
        if self.constraints:
            sql = "{sql} {constraints}".format(
                sql=sql,
                constraints=" ".join(self.constraints)
            )
        return sql



