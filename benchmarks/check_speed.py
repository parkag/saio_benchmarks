#!/usr/bin/python

import json
import numpy
import os
import optparse
import psycopg2
import sys
import signal
import time

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from sql_utils import prepare_for_geqo, prepare_for_saio, set_collapse_limits
from parameters import geqo_default_params_effort, saio_params


"""
Benchmarking script for SAIO/GEQO comparison.

It runs explain on provided queries multiple times using SAIO or GEQO
to find out plan quality (cost), planning time and memory usage.

Usage:
python check_speed.py --query explain.sql --loops 5 --timeout 1000
"""


def main():
    # Assuming schema is loaded into DB, run one query with SAIO and GEQO
    parser = optparse.OptionParser()
    parser.add_option("-l", "--loops", type=int)
    parser.add_option("-t", "--timeout", type=int)
    parser.add_option("--query")
    opts, args = parser.parse_args(sys.argv)

    print "Averaging over %d loops" % opts.loops

    query = file(opts.query).read()
    run_tests(
        geqo_default_params_effort, None, query, "query.geqo.out", 
        opts.loops, opts.timeout, optimizer="GEQO"
    )
    run_tests(
        saio_params, query, "query.saio.out",
        opts.loops, opts.timeout, optimizer="SAIO"
    )


def run_tests(test_params, setup_query, explain_query, path, loops, timeout, optimizer="SAIO"):
    try:
        os.unlink(path)
    except OSError:
        pass
    
    with psycopg2.connect(host="") as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        create_schema(conn, setup_query)
        
        k = []
        for i, params in enumerate(test_params):
            print "[%d/%d]" % (i, len(test_params))
            costs, times, memory = [], [], []
            
            setup_join_order_optimizer(conn, params, optimizer)
            
            for _ in xrange(loops):
                (
                    max_memory,
                    explain_result,
                    time 
                ) = run_test_case(conn, explain_query, timeout, optimizer)

                save_query_plan(explain_result, path, params)

                cost = get_cost(explain_result)
                
                memory.append(max_memory)
                costs.append(cost)
                times.append(time)
            
            output_result(
                path, params, numpy.average(costs),
                numpy.average(times), numpy.average(memory), optimizer
            )
            k.append(numpy.average(costs))
        print "avg cost: %lf" % numpy.average(k)


def create_schema(conn, setup_query):
    if not setup_query:
        return
    cur = conn.cursor()
    cur.execute(setup_query)


def setup_join_order_optimizer(conn, order_optimizer_parameters, optimizer):
    cur = conn.cursor()

    if optimizer == "SAIO":
        prepare_for_saio(cur, order_optimizer_parameters)
    elif optimizer == "GEQO":
        prepare_for_geqo(cur, order_optimizer_parameters)
    else:
        #unknown optimizer
        return

    set_collapse_limits(cur)


def run_test_case(conn, query, timeout, optimizer):
    pid = conn.get_backend_pid()
    signal.alarm(timeout)
    try:
        cost, time = check_time(conn, query, optimizer)
    except TimeoutException:
        cost, time = -1, -1
    signal.alarm(0)
    return max_memory(pid), cost, time


def save_query_plan(explain_result, path, parameters):
    fname = path + "_plan_"
    fname += "_".join([str(p) for p in parameters])
    with open(fname, 'w') as f:
        f.write(json.dumps(explain_result))


def check_time(conn, query, optimizer):
    cur = conn.cursor()

    t1 = time.time()
    cur.execute(query)
    t2 = time.time()

    explain_result = cur.fetchone()[0]

    execution_time = t2 - t1
    cur.close()

    return explain_result, float(execution_time)


def get_cost(explain_result):
    cost = explain_result[0]['Plan']['Total Cost']
    return cost


def max_memory(pid):
    for line in file('/proc/%d/status' % pid):
        if not line.startswith('VmPeak'):
            continue
        num, unit = line.split()[1:3]
        return int(num)


def output_result(path, params, cost, time, memory, optimizer):
    if optimizer == "SAIO":
        p = "SAIO (%d, %.3f, %.3f, %d)" % params
    elif optimizer == "GEQO":
        p = "GEQO (%d, %d, %d, %.3f)" % params
    
    print "Ran tests for %s, got %.5f %.5f %.5f" % (p, cost, time, memory)
    
    with file(path, 'a') as f:
        f.write(
            "%d\t%f\t%f\t%d\t%f\t%f\t%f\n" % (params + (cost, time, memory))
        )


class TimeoutException(Exception):
    pass


def handler(signum, frame):
    print 'ALARM'
    raise TimeoutException()


signal.signal(signal.SIGALRM, handler)


if __name__ == "__main__":
    main()
