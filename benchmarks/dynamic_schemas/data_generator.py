import random


class RandomTableDataGenerator(object):

    def __init__(self, table, nrows=1):
        self.table = table
        self.nrows = nrows

    def sql(self):
        sql = """INSERT INTO {table_name}({column_names})
            VALUES
                {values_data};""".format(
                    table_name=self.table.name,
                    column_names=", ".join(
                        column.name for column in self.table.columns),
                    values_data=''
                )
        return sql


class RandomSchemaDataGenerator(object):

    def __init__(self, tables):
        pass




def rand_bigint():
    return random.randint(1, 1E15)


def rand_boolean():
    return random.choice(['true', 'false'])


def rand_varchar():
    N = random.randint(1, 1000)
    charset = string.letters + string.digits
    return ''.join(random.choice(charset) for _ in range(N))


def rand_float8():
    return random.uniform(0, 1E10)


def rand_int4():
    return random.randint(1, 1E10)


standard_datatypes = [
    'bigint',
    'boolean',
    'varchar',
    'float8',
    'int4',
]

allowed_boolean_expressions = {
    'bigint' : [
        '{tab1} = {tab2}', 
        '{tab1} > {tab2}',
        '{tab1} < {tab2}',
        '{tab1} != {tab2}',
        '{tab1} % {tab2}',
    ],
    'boolean' : [
        '{tab1} = {tab2}',
        '{tab1} OR {tab2}',
        '{tab1} AND {tab2}',
        '{tab1} XOR {tab2}'
    ],
    'varchar' : [
        '{tab1} = {tab2}',
        '{tab1} LIKE {tab2}'
    ],
    'float8' : [
        '{tab1} = {tab2}', 
        '{tab1} > {tab2}',
        '{tab1} < {tab2}',
        '{tab1} != {tab2}'
    ],
    'int4' : [
        '{tab1} = {tab2}', 
        '{tab1} > {tab2}',
        '{tab1} < {tab2}',
        '{tab1} != {tab2}',
        '{tab1} % {tab2}',
    ]
}

record_generators = {
    'bigint': rand_bigint,
    'boolean': rand_boolean,
    'varchar': rand_varchar,
    'float8': rand_float8,
    'int4': rand_int4,
}