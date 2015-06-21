#saio_benchmarks

This repository contains a set of Python scripts that generate a set of large random queries.

These queries can be used to benchmark PostgreSQL join order optimizers like:

    * GEQO
    * SAIO

##Benchmarks

This repository contains code to run the benchmarks.

To run the benchmarks you should have installed all the dependencies:

    * Postgres (preferably 9.3+)
    * SAIO (https://github.com/parkag/saio)
    * psycopg2
    * numpy

The way I run these benchmarks is:
```
sudo su

su postgres

python benchmarks/benchmark_suite.py
```

##Report

To play with the report you also need:

    * IPython
    * IPython notebook
    * matplotlib

