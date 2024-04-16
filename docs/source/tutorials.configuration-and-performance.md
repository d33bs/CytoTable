---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.16.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# CytoTable configuration and performance tutorial

CytoTable performance may vary depending on the size and location of source data, available system resources, and how CytoTable is configured.
This tutorial will provide light guidance on how to configure CytoTable based on your system resources.
We focus this tutorial on two main configuration details which we observe as having the largest impact: __data chunk sizes__ and __Parsl configuration__.

- __Data chunk sizes__: CytoTable uses the `chunk_size` parameter to create row-wise "chunks" of data operations which limits the total amount of memory used by procedures ([see here for more information](overview.md#data-chunking)).
Larger chunk sizes can sometimes lead to faster time performance and larger memory footprints.
Smaller chunk sizes can lead to slower time performance and smaller memory footprints.
- __Parsl Configuration__: CytoTable uses [Parsl](https://parsl.readthedocs.io/en/stable/index.html) to efficiently process data through multi-step partially concurrent workflows and optional parallelism.
Parsl provides a number of different configuration options which may be specified to CytoTable through the `parsl_config` parameter.

The following is an example of how these configuration options are specified when using CytoTable:

```python
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

convert(
        source_path="source_data_path",
        dest_path="destination_data_path",
        dest_datatype="parquet",
        # Here we set the data chunk size to be 10,000.
        chunk_size=10000,
        # Here we use Config and ThreadPoolExecutor 
        # objects to configure Parsl for threaded 
        # execution with defaults.
        parsl_config=Config(
            executors=[
                ThreadPoolExecutor()
            ]
        )
    )
```

For more information, Parsl also provides [in-depth documentation on configuration](https://parsl.readthedocs.io/en/stable/userguide/index.html).

## Additional performance considerations

In addition to Parsl, a number of other elements may impact the performance you find with CytoTable.
These are presented in no particular order in terms of impact (this will depend largely on the data sources and system resources available which have a wide variation).

- __Cloud-based vs local data sources__: CytoTable provides source data capabilities through [cloudpathlib](https://cloudpathlib.drivendata.org/stable/), a package for interacting with cloud data storage through a unified API.
Cloud-based data sources will generally be processed more slowly than locally available data in CytoTable.
- __PyArrow settings__: CytoTable makes use of [PyArrow](https://arrow.apache.org/docs/python/index.html) for core in-memory data work for performance and integrative capabilities.
    - PyArrow provides the ability to use non-default memory allocation which can sometimes enable greater performance.
    CytoTable uses the default memory allocation selection performed by PyArrow. One may use the [`ARROW_DEFAULT_MEMORY_POOL`](https://arrow.apache.org/docs/cpp/env_vars.html#envvar-ARROW_DEFAULT_MEMORY_POOL) environment variable to specify which memory allocator is used by CytoTable (`jemalloc`, `mimalloc`, or `(C)malloc`)([see here for more](architecture.technical.md#arrow-memory-allocator-selection)).
    - PyArrow operations through CytoTable use memory mapping (for example [`parquet.read_table(memory_mapped=...)`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html)) and may be turned off through the `CYTOTABLE_ARROW_USE_MEMORY_MAPPING` environment variable ([see here for more](architecture.technical.md#arrow-memory-mapping-selection)).
- __DuckDB settings__: CytoTable uses [DuckDB](https://duckdb.org/docs/) to perform SQL-based data processing. DuckDB provides internal concurrency through the use of threads ([see here for more](https://duckdb.org/docs/connect/concurrency.html)).
The number of threads defaults to the number of processors available on the system.
Within CytoTable, the number of threads used by DuckDB may be explicitly set through the `CYTOTABLE_MAX_THREADS` environment variable.
Please note: DuckDB is not used to write data in parallel through CytoTable.

+++

## Definitions

We provide the following definitions to help clarify this content.

### Concurrency and parallelism
- [Concurrency](https://en.wikipedia.org/wiki/Concurrency_(computer_science)): the structure of computer program which allows for non-sequential execution without affecting the outcome.
- [Parallelism](https://en.wikipedia.org/wiki/Parallel_computing): a type of computation in which more than one calculation may take place at the same time.

### Processors and threads

- [Processor](https://en.wikipedia.org/wiki/Central_processing_unit): a computer resource used to execute instructions from computer software. Computers may have one or many processors.
- [Thread](https://en.wikipedia.org/wiki/Thread_(computing)): a sequence of computer software instructions executed by a processor. A processor may have one or many threads. A processor will only make progress one thread at a time.
- [Multiprocessing](https://en.wikipedia.org/wiki/Multiprocessing): the use of more than one processor to accomplish a software task.
- [Multithreading](https://en.wikipedia.org/wiki/Multithreading_(computer_architecture)): the use of more than one thread to accomplish a software task.

### Parsl

- [Parsl Executors](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.base.ParslExecutor.html): abstractions which represent computer resources available to accomplish tasks through Parsl.
- [`parsl.executors.ThreadPoolExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html): a  Parsl executor with multithreading capabilities.
- [`parsl.executors.HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html): a Parsl executor with multiprocessing capabilities.

+++

### Configuration performance heuristics

Decisions about CytoTable configuration may benefit from understanding common heuristics about computing and technologies implemented by CytoTable.
___Caveat emptor___: these are general guidance and may not be perfectly aligned to every system.

- __Benchmark large data work on "not too small" data subsets__: it can take a few attempts to get the correct configuration for performance optimization.
Save time by using a subset of your data to more quickly iterate through benchmarks of time and resource consumption.
At the same time, consider using a subset which is not "too" small to help demonstrate realistic performance findings.
- __Chunk sizes based on Parquet dataset file sizes:__ one way to estimate chunk size for a given source dataset is to use Parquet dataset file size.
Parquet files are often thought to have the best performance when their storage size is around `100 MB` - `1024 MB` (`1 GB`).
- __Best number of threads__: the number of threads used by software is typically set to the number of processors available (sometimes multiplied by small integer, as in the case of Python's [`ThreadPoolExecutor`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor).).
- __Multithreading involves "taking turns"__: multiple threads within the same process will "take turns" to accomplish a task (we won't see completion of tasks at the exact same time).
- __Multiprocessing is typicaly more resource intensive__: multiprocessing allows tasks to be completed in parallel but often involves the consumption of greater resources due to management overhead and how entire processors are utilized (in addition to threading behavior).
- __Avoid too much multitasking (multithread or multiprocess tasks)__: there are limits to the benefits received through multitasking.
Each task has an inherent resource cost overhead for management in addition to the work it will accomplish.
Be sure to consider reasonable numbers of multithreaded or multiprocessed tasks to avoid too much management overhead.

+++

### Demonstration

This tutorial will demonstrate the information above using code below.

#### Demonstration environment

This demonstration makes use of development dependencies of CytoTable.
We recommend cloning the repository and using the [Contributing getting started](contributing.md#getting-started) documentation for help recreating the environment used here.

#### Demonstration dataset

We leverage a "not too small" source data file from the [Cell Painting Gallery](https://github.com/broadinstitute/cellpainting-gallery) `cpg0016-jump` dataset ([preprint here](https://doi.org/10.1101/2023.03.23.534023)) to help demonstrate how configuration impacts performance within CytoTable.

+++

### How can we pick a "not too small" dataset?

We can use [file globbing](https://en.wikipedia.org/wiki/Glob_(programming)) through pathnames to determine a file which is small but not too small for use with performance benchmarking.

```{code-cell} ipython3
import pathlib

from cloudpathlib import S3Client

# set a path based on the above
cloud_source_data_path = (
    "s3://cellpainting-gallery/cpg0016-jump/"
    "source_4/workspace/backend/2021_08_23_Batch12/"
)

s3_client = S3Client(no_sign_request=True)

# show sorted results for recursively globbed sqlite filepaths based on filesize
sorted(
    {
        f"{path.parent.name}/{path.name}": f"{round(path.stat().st_size / 1024 / 1024 / 1024):02} GB"
        for path in s3_client.CloudPath(
            cloud_source_data_path,
        ).rglob("*.sqlite")
    }.items(),
    # sort by dictionary values
    key=lambda item: item[1],
)
```

```{code-cell} ipython3
# download a "not too small" dataset from the above information.
local_file = pathlib.Path("./BR00126114.sqlite")

# check if we already have the file, if not download it
if not local_file.is_file():
    s3_client.CloudPath(
        cloud_source_data_path + "BR00126114/BR00126114.sqlite",
    ).download_to(destination=".")

# show the local file
local_file
```

### How can we determine what chunk size to start with?

We can use knowledge about the dataset table row length and export to file using varying chunk sizes to find a file size between 100 MB - 1 GB.

```{code-cell} ipython3
import sqlite3
from contextlib import closing

table_row_counts = {}

# gather table names from the data
with closing(sqlite3.connect(str(local_file))) as cx:
    # We build a contextlib.closing context to close the database
    # connection automatically instead of closing explicitly.
    with cx:
        cursor = cx.execute(
            """
            /* we use a special SQLite reference here
            called `sqlite_master` to attain metadata
            about the database */
            SELECT tbl_name
            FROM sqlite_master 
            WHERE type='table';
            """
        )
        table_names = [elem[0] for elem in cursor.fetchall()]

        # loop using each table name
        for table_name in table_names:
            cursor = cx.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                """
            )
            table_row_counts[table_name] = cursor.fetchone()[0]

# show the table names and row counts
table_row_counts
```

```{code-cell} ipython3
import duckdb

chunk_size_to_try = 10000
benchmark_filesize_file = "example-benchmark.parquet"


def export_sqlite_table_rows_to_parquet(
    sqlite_file: str, chunk_size: int, parquet_filename: str
) -> str:
    # use duckdb to extract a "chunk" of data using chunk_size
    # and exporting to Parquet file.
    with duckdb.connect() as ddb:
        ddb.execute(
            """
            /* Install and load sqlite plugin for duckdb */
            INSTALL sqlite_scanner;
            LOAD sqlite_scanner;
            """
        )
        ddb.execute(
            f"""
            COPY (
                SELECT *
                /* duckdb allows us to use a special function to
                access SQLite database tables directly as seen here */
                FROM sqlite_scan({str(sqlite_file)}, 'Cells')
                LIMIT {chunk_size}
            )
            /* here we export to a file of parquet format type */
            TO '{parquet_filename}'
            (FORMAT PARQUET)
            """
        )
    return parquet_filename


parquet_filename = export_sqlite_table_rows_to_parquet(
    sqlite_file=local_file,
    chunk_size=chunk_size_to_try,
    parquet_filename=benchmark_filesize_file,
)

print(
    f"Parquet file size with chunk size of {chunk_size_to_try}:",
    round(pathlib.Path(parquet_filename).stat().st_size / 1024 / 1024),
    "MB",
)
```

### How can we estimate the number of threads or processes to use?

We can use the number of processors on the system as a rough estimate for number of threads to use.
Recall that this is only an heuristic as the number of possible threads to use is much higher but may come with an imbalance from thread management overhead (or other aspects).

```{code-cell} ipython3
import multiprocessing

number_of_threads_to_try = multiprocessing.cpu_count()
number_of_processors_to_try = multiprocessing.cpu_count()
print(
    f"Threads to try: {number_of_threads_to_try}",
    f"Processors to try: {number_of_processors_to_try}",
    sep="\n",
)
```

### How does multithreaded performance change with different configurations?

We can use [`parsl.executors.ThreadPoolExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html) to test various numbers of threads and other configuration to see what happens.

```{code-cell} ipython3
print(
    f"Threads: {number_of_threads_to_try}", f"Chunk size: {chunk_size_to_try}", sep="\n"
)
```

```{code-cell} ipython3
# set identifying columns to use
identifying_columns = (
    "TableNumber",
    "ImageNumber",
    "Parent_Cells",
    "Parent_Nuclei",
    "Cytoplasm_Parent_Cells",
    "Cytoplasm_Parent_Nuclei",
    "Cells_ObjectNumber",
    "Nuclei_ObjectNumber",
)
```

```{code-cell} ipython3
%%timeit -r 1 -n 1

from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

import cytotable

destination_path = str(local_file.name).replace(".sqlite", ".parquet")

result = cytotable.convert(
    source_path=str(local_file),
    dest_path=str(local_file.name).replace(".sqlite", ".parquet"),
    dest_datatype="parquet",
    preset="cell-health-cellprofiler-to-cytominer-database",
    chunk_size=chunk_size_to_try,
    parsl_config=Config(
        executors=[ThreadPoolExecutor(max_threads=number_of_threads_to_try)]
    ),
    identifying_columns=identifying_columns,
)
result
```

#### What happens if we increase the chunk size?

What happens if we increase the chunk size to the upper boundary of the Parquet size heuristic (100 MB - 1 GB) and have only one chunk per table?

```{code-cell} ipython3
print(
    f"Threads: {number_of_threads_to_try}",
    f"Chunk size: {chunk_size_to_try * 8}",
    sep="\n",
)
```

```{code-cell} ipython3
# show the estimated parquet file size with a larger chunk size
parquet_filename = export_sqlite_table_rows_to_parquet(
    sqlite_file=local_file,
    chunk_size=chunk_size_to_try * 8,
    parquet_filename=benchmark_filesize_file,
)

print(
    f"Parquet file size with chunk size of {chunk_size_to_try * 8}:",
    round(pathlib.Path(parquet_filename).stat().st_size / 1024 / 1024),
    "MB",
)
```

```{code-cell} ipython3
import parsl

# remove any previous output
destination_path = str(local_file.name).replace(".sqlite", ".parquet")
pathlib.Path(destination_path).unlink(missing_ok=True)

# reset parsl config for fair comparisons
parsl.clear()
```

```{code-cell} ipython3
%%timeit -r 1 -n 1

from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

import cytotable

destination_path = str(local_file.name).replace(".sqlite", ".parquet")

# remove any previous output
pathlib.Path(destination_path).unlink(missing_ok=True)

result = cytotable.convert(
    source_path=str(local_file),
    dest_path=destination_path,
    dest_datatype="parquet",
    preset="cell-health-cellprofiler-to-cytominer-database",
    chunk_size=chunk_size_to_try * 8,
    parsl_config=Config(
        executors=[ThreadPoolExecutor(max_threads=number_of_threads_to_try)]
    ),
    identifying_columns=identifying_columns,
)
result
```

```{code-cell} ipython3
# remove any previous output
destination_path = str(local_file.name).replace(".sqlite", ".parquet")
pathlib.Path(destination_path).unlink(missing_ok=True)

# reset parsl config for fair comparisons
parsl.clear()
```

### How does multprocessed performance change with different configurations?

We can use [`parsl.executors.HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html) to test various numbers of blocks and other configuration to see what happens.

```{code-cell} ipython3
print(
    f"Processors: {number_of_processors_to_try}",
    f"Chunk size: {chunk_size_to_try}",
    sep="\n",
)
```

```{code-cell} ipython3
%%timeit -r 1 -n 1

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

import cytotable

destination_path = str(local_file.name).replace(".sqlite", ".parquet")

result = cytotable.convert(
    source_path=str(local_file),
    dest_path=destination_path,
    dest_datatype="parquet",
    preset="cell-health-cellprofiler-to-cytominer-database",
    chunk_size=chunk_size_to_try,
    parsl_config=Config(
        executors=[
            HighThroughputExecutor(
                max_workers_per_node=1,
                provider=LocalProvider(
                    max_blocks=number_of_processes_to_try,
                ),
            )
        ]
    ),
    identifying_columns=identifying_columns,
)
result
```

```{code-cell} ipython3
# remove any previous output
import shutil

import parsl

destination_path = str(local_file.name).replace(".sqlite", ".parquet")
shutil.rmtree(destination_path)
pathlib.Path(destination_path).unlink(missing_ok=True)

# reset parsl config for fair comparisons
parsl.clear()
```

```{code-cell} ipython3
# reset parsl config for fair comparisons
parsl.clear()
```

```{code-cell} ipython3
%%timeit -r 1 -n 1

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

import cytotable

destination_path = str(local_file.name).replace(".sqlite", ".parquet")

result = cytotable.convert(
    source_path=str(local_file),
    dest_path=destination_path,
    dest_datatype="parquet",
    preset="cell-health-cellprofiler-to-cytominer-database",
    chunk_size=chunk_size_to_try,
    parsl_config=Config(
        executors=[
            HighThroughputExecutor(
                label="local_htex",
                max_workers_per_node=2,
                provider=LocalProvider(
                    min_blocks=1,
                    init_blocks=1,
                    max_blocks=2,
                    nodes_per_block=1,
                    parallelism=1,
                ),
            )
        ]
    ),
    identifying_columns=identifying_columns,
)
result
```

```{code-cell} ipython3
# 10min 36s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)
"""
HighThroughputExecutor(
                provider=LocalProvider(
                    init_blocks=round(number_of_processes_to_try / 2),
                    max_blocks=round(number_of_processes_to_try / 2),
                ),
            )
"""
# 9min 32s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each) - with
"""
HighThroughputExecutor(
                cores_per_worker=1,
                max_workers_per_node=1,
                provider=LocalProvider(
                    nodes_per_block=1,
                    init_blocks=round(number_of_processes_to_try / 2),
                    max_blocks=round(number_of_processes_to_try / 2),
                ),
            )
"""
# 9min 54s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)
"""
HighThroughputExecutor(
                cores_per_worker=1,
                max_workers_per_node=1,
                provider=LocalProvider(
                    nodes_per_block=1,
                    init_blocks=number_of_processes_to_try,
                    max_blocks=number_of_processes_to_try,
                ),
            )
"""
```

```console


to_parquet
--> _gather_sources.result() (join app)
--> _get_table_chunk_offsets.result() (python_app)
--> _get_table_columns_and_types.result() (python_app)


```
