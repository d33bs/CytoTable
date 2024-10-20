# Technical Architecture

Documentation covering technical architecture for CytoTable.

## Workflow Technologies

CytoTable uses [Prefect](https://docs.prefect.io/) to execute collections of tasks as workflows.
In Prefect, Tasks are isolated pieces of work stored within a Python function which is decorated with the `@task` decorator.
Workflows, or flows, are collections of one or more tasks and are decorated using the `@flow` decorator.
See the following documentation for more information on how tasks and flows may be used within Prefect: [Prefect Documentation: Tutorial: First steps](https://docs.prefect.io/tutorials/first-steps/)

### Task Execution

Flows and tasks within CytoTable may be executed using [Prefect Task Executors](https://docs.prefect.io/tutorials/execution/).
By default, CytoTable assumes local task execution with [SequentialTaskRunner](https://docs.prefect.io/tutorials/execution/#sequential-execution).

For greater scalability, CytoTable may also be used with concurrent task runners.
Please note: using concurrent task runners may require implementing a standalone `prefect orion` server to ensure SQLite write errors are not experienced (more information on this topic may be found under [Prefect#7277](https://github.com/PrefectHQ/prefect/issues/7277)).
Examples of concurrent task runners include [ConcurrentTaskRunner](https://docs.prefect.io/tutorials/execution/#concurrent-execution), a built-in option for concurrent operiations, and [prefect-dask DaskTaskRunner](https://prefecthq.github.io/prefect-dask/) which can further help scale parallel execution using a [Dask environment](https://docs.dask.org/en/stable/) (local or otherwise).

## Data Technologies

### Data Paths

Data source paths handled by CytoTable may be local or cloud-based paths.
Local data paths are handled using [Python's Pathlib](https://docs.python.org/3/library/pathlib.html) module.
Cloud-based data paths are managed by [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/).
Reference the following page for how cloudpathlib client arguments may be used: [Overview: Data Source Locations](overview.md#data-source-locations)

### In-process Data Format

In addition to using Python native data types, we also accomplish internal data management for CytoTable using [PyArrow (Apache Arrow) Tables](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
Using Arrow-compatible formats is intended to assist cross-platform utility, encourage high-performance, and enable advanced data integration with non-Python tools.

### SQL-based Data Management

We use the [DuckDB Python API client](https://duckdb.org/docs/api/python/overview) in some areas to interface with [SQL](https://en.wikipedia.org/wiki/SQL) (for example, SQLite databases) and other data formats.
We use DuckDB SQL statements to organize joined datasets or tables as Arrow format results.
