import random
from check_speed import run_tests
from parameters import (
    geqo_default_params_effort,
    saio_params
)
from dynamic_schemas import star
from dynamic_schemas.query_generator import *


LOOPS = 1
TIMEOUT = 4000


def log_object(fname, content):
    with open(fname, 'w') as f:
        read_data = f.write(content)


def test_star_query_cartesian(name, arms):
    setup_query = star.get_schema_query(arms)
    query = star.get_analyze_query2(arms)

    print query
    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort, 
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out",
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_star_query_join(name, arms):
    setup_query = star.get_schema_query(arms)
    query = star.get_analyze_query2_join(arms)

    print query
    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort, 
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out",
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_moderate_query():
    setup_query = file('../schemas/schema.sql').read()
    setup_query += file('../schemas/view.sql').read()

    query = file('../queries/explain.sql').read()

    run_tests(
        geqo_default_params_effort[:6],
        setup_query,
        query,
        "moderate_query.geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        "moderate_query.saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_complex_query():
    # this is not so complex, the other one is complex
    setup_query = file('../schemas/dump.sql').read()
    setup_query = None
    query = file('../queries/robert.sql').read()

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        "complex_query.geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        "complex_query.saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )    


def test_random_query(name, ntables, joins, left_joins, right_joins):    
    s = RandomSchema(ntables, '')
    s.generate_tables()
    q = RandomQuery(s, joins, left_joins, right_joins)

    setup_query = s.sql()
    query = q.explain_sql()

    print query
    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_random_query_datatypes(name, ntables, joins, left_joins, right_joins):
    from dynamic_schemas.data_generator import standard_datatypes

    s = RandomSchema(ntables, 0, datatypes=['text', 'int', 'float'])
    s.generate_tables(min_cols=1, max_cols=3)
    q = RandomQuery(s, joins, left_joins, right_joins)

    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query
    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )

def test_random_indexed_query(
        name, ntables, joins, left_joins, right_joins, indexes):
    from dynamic_schemas.data_generator import standard_datatypes

    s = RandomSchema(ntables, indexes, datatypes=['text', 'int', 'float'])
    s.generate_tables(min_cols=1, max_cols=3)
    s.generate_indexes()
    q = RandomQuery(s, joins, left_joins, right_joins)

    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )

def test_random_nested_query(
        name, ntables, joins, left_joins, right_joins, indexes, nest_level):    
    
    s = RandomSchema(ntables, ntables/10, datatypes=['text', 'int'])
    s.generate_tables(min_cols=2, max_cols=4)
    s.generate_indexes()

    q = RandomNestedQuery(s, joins, left_joins, right_joins, nest_level)

    setup_query = s.sql()
    query = q.explain_sql()

    print query
    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )   


def test_random_complicated_query(
        name, ntables, joins, left_joins, right_joins, indexes):
    from dynamic_schemas.data_generator import standard_datatypes

    s = RandomSchema(ntables, indexes, datatypes=standard_datatypes)
    s.generate_tables(min_cols=2, max_cols=5)
    s.generate_indexes()
    q = RandomComplicatedQuery(s, joins, left_joins, right_joins)

    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_random_nested_improved_query(
        name, ntables, joins, left_joins, right_joins, indexes):
    from dynamic_schemas.data_generator import standard_datatypes

    s = RandomSchema(ntables, indexes, datatypes=standard_datatypes)
    s.generate_tables(min_cols=2, max_cols=5)
    s.generate_indexes()
    q = NestedQueryImproved(s, joins, left_joins, right_joins)

    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_double_nested_query(name):

    s = RandomSchema(30, 10, datatypes=['text'])
    s.generate_tables(min_cols=3, max_cols=4)
    s.generate_indexes()
    q = FixedDoubleNestedQuery()

    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )

def test_regular_nested_query(name, joins, left_joins, right_joins, subqueries, join_type):
    s = RandomSchema(50, 10, datatypes=['text'])
    s.generate_tables(min_cols=3, max_cols=4)
    s.generate_indexes()

    q = RegularNestedQuery(s, joins, left_joins, right_joins, subqueries, join_type)
    setup_query = s.sql()
    print setup_query
    query = q.explain_sql()
    print query

    log_object(name+".schema", setup_query)
    log_object(name+".query", query)

    run_tests(
        geqo_default_params_effort,
        setup_query,
        query,
        name+".geqo.out", 
        LOOPS,
        TIMEOUT,
        optimizer="GEQO"
    )
    run_tests(
        saio_params,
        setup_query,
        query,
        name+".saio.out", 
        LOOPS,
        TIMEOUT,
        optimizer="SAIO"
    )


def test_star():
    test_star_query_cartesian('star_query_30_arms', arms=30)
    test_star_query_cartesian('star_query_50_arms', arms=50)
    test_star_query_cartesian('star_query_80_arms', arms=80)
    test_star_query_cartesian('star_query_100_arms', arms=100)

    test_star_query_join('star_query_join_30_arms', arms=30)
    test_star_query_join('star_query_join_50_arms', arms=50)
    test_star_query_join('star_query_join_80_arms', arms=80)


def test_provided_queries():
    test_moderate_query()# this is complex actually
    #test_complex_query() # this is not so complex


def test_flat_queries():
    """
    test_random_query_datatypes('random_query_datatypes_20_joins', 30, 20, 0, 0)
    test_random_query_datatypes('random_query_datatypes_30_joins', 40, 30, 0, 0)
    test_random_query_datatypes('random_query_datatypes_50_joins', 60, 50, 0, 0)
    test_random_query_datatypes('random_query_datatypes_80_joins', 90, 80, 0, 0)
    test_random_query_datatypes('random_query_datatypes_80_left_joins', 90, 0, 80, 0)
    test_random_query_datatypes('random_query_datatypes_80_right_joins', 90, 0, 0, 80)

    test_random_query('random_query_15_joins_no_constraints', 20, 15, 0, 0)
    test_random_query('random_query_15_left_joins_no_constraints', 20, 0, 15, 0)
    test_random_query('random_query_15_right_joins_no_constraints', 20, 0, 0, 15)
    test_random_query('random_query_5_joins_5_left_5_right', 20, 5, 5, 5)

    test_random_query('random_query_20_joins_no_constraints', 25, 20, 0, 0)
    test_random_query('random_query_20_left_joins_no_constraints', 25, 0, 20, 0)
    test_random_query('random_query_20_right_joins_no_constraints', 25, 0, 0, 20)
    test_random_query('random_query_10_joins_5_left_5_right', 25, 10, 5, 5)

    test_random_query('random_query_30_joins_no_constraints', 35, 30, 0, 0)
    test_random_query('random_query_30_left_joins_no_constraints', 35, 0, 30, 0)
    test_random_query('random_query_30_right_joins_no_constraints', 35, 0, 0, 30)
    test_random_query('random_query_10_joins_10_left_10_right', 35, 10, 10, 10)

    test_random_query('random_query_50_joins_no_constraints', 55, 50, 0, 0)
    test_random_query('random_query_50_left_joins_no_constraints', 55, 0, 50, 0)
    test_random_query('random_query_50_right_joins_no_constraints', 55, 0, 0, 50)
    test_random_query('random_query_20_joins_15_left_15_right', 55, 20, 15, 15)

    test_random_query('random_query_70_joins_no_constraints', 75, 70, 0, 0)
    test_random_query('random_query_70_left_joins_no_constraints', 75, 0, 70, 0)
    test_random_query('random_query_70_right_joins_no_constraints', 75, 0, 0, 70)
    test_random_query('random_query_30_joins_20_left_20_right', 75, 30, 20, 20)

    test_random_query('random_query_100_joins_no_constraints', 105, 100, 0, 0)
    test_random_query('random_query_100_left_joins_no_constraints', 105, 0, 100, 0)
    test_random_query('random_query_100_right_joins_no_constraints', 105, 0, 0, 100)
    test_random_query('random_query_40_joins_30_left_30_right', 105, 40, 30, 30)"""

    #test_random_query('random_query_130_joins_no_constraints', 135, 130, 0, 0)
    test_random_query('random_query_130_left_joins_no_constraints', 135, 0, 130, 0)
    test_random_query('random_query_130_right_joins_no_constraints', 135, 0, 0, 130)
    test_random_query('random_query_50_joins_40_left_40_right', 135, 50, 40, 40)
    """
    test_random_indexed_query(
      'random_query_15_joins_5_indexes', 30, 15, 0, 0, 5)
    test_random_indexed_query(
        'random_query_20_joins_10_indexes', 35, 20, 0, 0, 10)
    test_random_indexed_query(
        'random_query_30_joins_15_indexes', 45, 30, 0, 0, 15)
    test_random_indexed_query(
        'random_query_50_joins_20_indexes', 65, 50, 0, 0, 20)
    test_random_indexed_query(
        'random_query_20_joins_15_left_15_right', 85, 20, 15, 15, 25)
    """

def test_nested_queries():
    #test_random_nested_query(
    #    'random_nested_query_15_joins', 20, 15, 0, 0, 15, 1)
    #test_random_nested_query(
    #    'random_nested_query_15_left_joins', 20, 0, 15, 0, 15, 1)
    #test_random_nested_query(
    #    'random_nested_query_15_right_joins', 20, 0, 0, 15, 15, 1)
    """test_random_nested_query(
        'random_nested_query_5_joins_5_left_5_right', 20, 5, 5, 5, 15, 1)
    
    test_random_nested_query(
        'random_nested_query_20_joins', 25, 20, 0, 0, 15, 1)
    test_random_nested_query(
        'random_nested_query_20_left_joins', 25, 0, 20, 0, 15, 1)
    test_random_nested_query(
        'random_nested_query_20_right_joins', 25, 0, 0, 20, 15, 1)   """
    """test_random_nested_query(
        'random_nested_query_30_joins', 40, 30, 0, 0, 15, 1)
    test_random_nested_query(
        'random_nested_query_30_left_joins', 40, 0, 30, 0, 15, 1)
    test_random_nested_query(
        'random_nested_query_30_right_joins', 40, 0, 0, 30, 15, 1)"""
    #test_random_nested_query(
    #    'random_nested_query_10_joins_10_left_10_right', 40, 10, 10, 10, 15, 1)
    
    """test_random_nested_query(
        'random_nested_query_50_joins', 55, 50, 0, 0, 35, 1)"""
    #test_random_nested_query(
    #    'random_nested_query_50_left_joins', 55, 0, 50, 0, 35, 1)
    #test_random_nested_query(
    #    'random_nested_query_50_right_joins', 55, 0, 0, 50, 35, 1)

    """test_random_nested_query(
        'random_nested_query_20_joins_15_left_15_right', 55, 20, 15, 15, 35, 1)"""
    
    #test_random_nested_query(
    #    'random_nested_query_80_left_joins', 95, 80, 0, 0, 25, 1)

    #test_random_nested_query(
    #    'random_nested_query_20_joins_20_left_20_right', 65, 20, 20, 20, 15, 1)

    #test_random_nested_query(
    #    'random_nested_query_10_joins_30_left_30_right', 85, 10, 30, 30, 25, 1)

    #test_random_nested_improved_query(
    #    'random_nested_query_improved_20_joins_15_left_10_right', 60, 20, 15, 10, 20)

    #doesn't work
    #test_random_complicated_query(
    #    'random_complicated_query_20_joins_5_left_5_right', 45, 20, 5, 5, 15)

    #test_regular_nested_query(
    #    'regular_nested_query_2_joins_2_left_2_right_5_subqueries', 2, 2, 2, 5, 'join')
    #test_regular_nested_query(
    #    'regular_nested_query_3_joins_3_left_1_right_3_subqueries', 3, 3, 1, 3, 'join')
    #test_regular_nested_query(
    #    'regular_nested_query_3_joins_3_left_3_right_4_subqueries', 3, 3, 3, 4, 'join')
    #test_regular_nested_query(
    #    'regular_nested_query_1_joins_1_left_5_right_6_subqueries', 1, 1, 5, 6, 'join')
    #test_double_nested_query('double_nested_query')
    #test_regular_nested_query(
    #    'regular_nested_query_1_joins_1_left_5_right_6_subqueries', 1, 1, 5, 6, 'right join')


def main():
    #test_star()
    test_provided_queries()
    #test_flat_queries()
    
    #test_nested_queries()


if __name__ == "__main__":
    main()

