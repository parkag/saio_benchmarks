# saio_benchmarks
This repository contains a set of Python scripts that generate a set of large random queries.

These queries can be used to benchmark PostgreSQL join order optimizers like:

* GEQO
* SAIO

This repository contains code to run the benchmarks.

The way I run these benchmarks is:

```
sudo su

su postgres

python benchmarks/benchmark_suite.py
```

