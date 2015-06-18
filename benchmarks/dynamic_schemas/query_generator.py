from schema_generator import *


class RandomQuery(object):

    """Basic approach. Output is like
    SELECT * FROM tab_1 
        JOIN tab_2 on (tab_1.col_x = tab_2.col_y)
        JOIN ...
        LEFT JOIN tab ...
        ...
        RIGHT JOIN tab ...
    """
    def __init__(self, schema, joins, left_joins, right_joins):
        self.schema = schema
        self.joins = joins
        self.left_joins = left_joins
        self.right_joins = right_joins
        self.already_joined = []

    def _get_from_section(self):
        from_section = ' {tab_name} '.format(
            tab_name=self.schema.tables[0].name
        )
        
        self.already_joined.append(self.schema.tables[0])
        
        for i in xrange(self.joins):
            from_section += self._get_join_part("JOIN")
        for i in xrange(self.left_joins):
            from_section += self._get_join_part("LEFT JOIN")
        for i in xrange(self.right_joins):
            from_section += self._get_join_part("RIGHT JOIN")
        
        return from_section

    def _get_join_part(self, join_type):
        n_tables = len(self.schema.tables)
        table1 = random.choice(self.already_joined)
        table2 = random.choice(self.schema.tables)

        while table2 in self.already_joined:
            table2 = random.choice(self.schema.tables)

        col1 = random.choice(table1.columns)
        col2 = random.choice(table2.columns)

        while col2.datatype != col1.datatype:
            table2 = random.choice(self.schema.tables)
            while table2 in self.already_joined:
                table2 = random.choice(self.schema.tables)
            col2 = random.choice(table2.columns)

        join_part = " {join_type} {tab} ON {boolean_expression}\n".format(
            join_type=join_type,
            tab=table2.name,
            boolean_expression=self._get_boolean_expression(
                table1, table2, col1, col2)
        )
        self.already_joined.append(table2)
        return join_part

    def _get_boolean_expression(self, table1, table2, col1, col2):
        """extend this"""
        return "{table1}.{col1} = {table2}.{col2}".format(
            table1=table1.name,
            table2=table2.name,
            col1=col1.name,
            col2=col2.name
        )

    def sql(self):
        sql = """SELECT * FROM {from_section}
        """.format(from_section=self._get_from_section())
        return sql

    def explain_sql(self):
        sql = """EXPLAIN (FORMAT JSON) \n""" + self.sql() + ";"
        return sql 


class RandomNestedQuery(RandomQuery):
    """Generated queries with subqueries"""

    def __init__(
        self, schema, joins, left_joins, right_joins, nest_level=1):
        RandomQuery.__init__(
            self, schema, joins, left_joins, right_joins)
        self.nest_level = nest_level

    def _get_join_part(self, join_type):
        n_tables = len(self.schema.tables)
        table1 = self.schema.tables[0]
        table2 = random.choice(self.schema.tables[n_tables/2:])

        join_part = ''
        if random.random() > 0.9:
            if table2 in self.already_joined:
                join_part = """ {join_type} ({subquery}) AS {subquery_name} 
                    ON TRUE\n""".format(
                    join_type=join_type,
                    subquery=self._get_subquery(),
                    subquery_name='subquery_'+str(table1.name)+'_'+str(random.randint(1,1000))
                )
        else:
            join_part = RandomQuery._get_join_part(self, join_type)
        return join_part

    def _get_subquery(self):
        n_tables = len(self.schema.tables)
        subquery_schema = RandomSchema(n_tables/2, 0, datatypes=self.schema.datatypes)
        
        if self.nest_level == 1:
            query = RandomQuery(
                self.schema, self.joins/15, self.left_joins/15, self.right_joins/15)
        else:
            query = RandomNestedQuery(
                subquery_schema, self.joins/5, self.joins/5, self.joins/5, self.nest_level-1)
        return query.sql()


class RandomComplicatedQuery(RandomQuery):

    """DOESNT WORK YET"""
    
    def __init__(self, schema, joins, left_joins, right_joins):
        RandomQuery.__init__(
            self, schema, joins, left_joins, right_joins)

    def _get_from_section(self):
        from_section = ' {tab_name} '.format(
            tab_name=self.schema.tables[0].name
        )
        
        self.already_joined.append(self.schema.tables[0])
        
        for i in xrange(self.joins):
            from_section += self._get_join_part("JOIN")
        for i in xrange(self.left_joins):
            from_section += self._get_join_part("LEFT JOIN")
        for i in xrange(self.right_joins):
            from_section += self._get_join_part("RIGHT JOIN")
        
        return from_section

    def _get_join_part(self, join_type):
        n_tables = len(self.schema.tables)
        table1 = random.choice(self.already_joined)
        table2 = random.choice(self.schema.tables)

        while table2 in self.already_joined:
            table2 = random.choice(self.schema.tables)

        col1 = random.choice(table1.columns)
        col2 = random.choice(table2.columns)

        while col2.datatype != col1.datatype:
            table2 = random.choice(self.schema.tables)
            while table2 in self.already_joined:
                table2 = random.choice(self.schema.tables)
            col2 = random.choice(table2.columns)

        join_part = """ {join_type}(SELECT {tab2}.{col2}, COUNT(*) from {tab2} 
                JOIN {tab1} on {tab1}.{col1} = {tab2}.{col2}) AS {subquery_name}
                ON TRUE\n""".format(
            subquery_name="subquery_"+str(random.randint(1,100))+"_"+str(random.randint(1,100)),
            join_type=join_type,
            tab1=table1.name,
            tab2=table2.name,
            col1=col1.name,
            col2=col2.name
        )
        self.already_joined.append(table2)
        return join_part

class NestedQueryImproved(RandomQuery):

    def __init__(self, schema, joins, left_joins, right_joins):
        self.schema = schema
        self.joins = joins
        self.left_joins = left_joins
        self.right_joins = right_joins
        self.already_joined = []
        self.nest_level = 1

    def _get_from_section(self):
        from_section = ' {tab_name} '.format(
            tab_name=self.schema.tables[0].name
        )
        
        self.already_joined.append(self.schema.tables[0])
        
        for i in xrange(self.joins):
            from_section += self._get_join_part("JOIN")
        for i in xrange(self.left_joins):
            from_section += self._get_join_part("LEFT JOIN")
        for i in xrange(self.right_joins):
            from_section += self._get_join_part("RIGHT JOIN")
        
        return from_section

    def _get_join_part(self, join_type):
        n_tables = len(self.schema.tables)
        table1 = self.schema.tables[0]
        table2 = random.choice(self.schema.tables[n_tables*2/3:])

        join_part = ''
        if random.random() > 0.7:
            if table2 in self.already_joined:
                join_part = """ {join_type} ({subquery}) AS {subquery_name} 
                    ON TRUE\n""".format(
                    join_type=join_type,
                    subquery=self._get_subquery(),
                    subquery_name='subquery_'+str(table1.name)+'_'+str(random.randint(1,1000))
                )
            else:
                table2 = able2 = random.choice(self.schema.tables[n_tables*2/3:])
        else:
            join_part = RandomQuery._get_join_part(self, join_type)
        return join_part

    def _get_subquery(self):
        n_tables = len(self.schema.tables)
        subquery_schema = RandomSchema(n_tables/2, 0, datatypes=self.schema.datatypes)
        
        if self.nest_level == 1:
            query = RandomQuery(
                self.schema, self.joins/10, self.left_joins/10, self.right_joins/10)
        else:
            query = RandomNestedQuery(
                subquery_schema, self.joins/5, self.joins/5, self.joins/5, self.nest_level-1)
        return query.sql()

    def _get_boolean_expression(self, table1, table2, col1, col2):
        """extend this"""
        return "{table1}.{col1} = {table2}.{col2}".format(
            table1=table1.name,
            table2=table2.name,
            col1=col1.name,
            col2=col2.name
        )

    def _get_what_section(self):
        what_section = '{tab}.{col}'.format(
            tab=self.schema.tables[0].name,
            col=self.schema.tables[0].columns[0].name)
        for i in xrange(20):
            table = random.choice(self.already_joined)
            column = random.choice(table.columns)
            what_section += ', {tab}.{col}'.format(
                tab=table.name,
                col=column.name)
        return what_section

    def sql(self):
        sql = """SELECT {what_section} FROM {from_section}
        """.format(
            from_section=self._get_from_section(),
            what_section=self._get_what_section(),
        )
        return sql

    def explain_sql(self):
        sql = """EXPLAIN (FORMAT JSON) \n""" + self.sql() + ";"
        return sql 


class RandomDoubleNestedQuery(object):

    def sql(self):
        sql = """SELECT * FROM table_1
        JOIN table_2 ON table_1.col_1 = table_2.col_1
        LEFT JOIN table_3 ON table_3.col_2 = table_2.col_2
        RIGHT JOIN table_4 ON table_4.col_1 = table_3.col_1
        RIGHT JOIN (
            SELECT * FROM table_5
                RIGHT JOIN table_6 ON table_6.col_1 = table_5.col_1
                JOIN (
                    SELECT * FROM table_7
                        JOIN table_8 ON table_8.col_2 = table_7.col_2
                        RIGHT JOIN table_9 ON table_9.col_1 = table_8.col_2
                        JOIN table_10 ON table_10.col_2 = table_9.col_2
                        RIGHT JOIN table_11 ON table_11.col_1 = table_7.col_1
                ) AS subsubquery_1 ON TRUE
        ) AS subquery_1 ON TRUE
        RIGHT JOIN (
            SELECT * FROM table_12
                RIGHT JOIN table_13 ON table_13.col_1 = table_12.col_1
                RIGHT JOIN table_14 ON table_14.col_2 = table_13.col_1
                RIGHT JOIN (
                    SELECT * FROM table_15
                        RIGHT JOIN table_16 ON table_16.col_2 = table_15.col_1
                        LEFT JOIN table_17 ON table_17.col_1 = table_15.col_1
                        RIGHT JOIN table_18 ON table_18.col_2 = table_15.col_2
                        RIGHT JOIN table_19 ON table_19.col_1 = table_18.col_1
                ) AS subsubquery_2 ON TRUE
                RIGHT JOIN table_20 ON table_20.col_1 = table_14.col_1
                RIGHT JOIN table_21 ON table_21.col_2 = table_20.col_2
        ) AS subquery_2 ON TRUE
        RIGHT JOIN table_22 ON table_22.col_1 = table_1.col_1
        LEFT JOIN table_23 ON table_23.col_2 = table_22.col_1
        """
        return sql

    def explain_sql(self):
        sql = """EXPLAIN (FORMAT JSON) \n""" + self.sql() + ";"
        return sql
