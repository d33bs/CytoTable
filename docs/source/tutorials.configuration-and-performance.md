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

# CytoTable Configuration and Performance

CytoTable performance may vary depending on the size and location of source data, available system resources, and how CytoTable is configured.
This tutorial will provide light guidance on how to configure Parsl for CytoTable based on your system resources.

CytoTable uses [Parsl](https://parsl.readthedocs.io/en/stable/index.html) to efficiently process data through multi-step partially concurrent workflows and optional parallelism.
Parsl provides a number of different configuration options which may be specified to CytoTable through the `parsl_config` parameter.

For example:

```python
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

convert(
        source_path="source_data_path",
        dest_path="destination_data_path",
        dest_datatype="parquet",
        # Here we use a Config and ThreadPoolExecutor object
        # to configure Parsl for threaded execution with defaults.
        parsl_config=Config(
            executors=[
                ThreadPoolExecutor()
            ]
        )
    )
```

For more information, Parsl also provides [in-depth documentation on configuration](https://parsl.readthedocs.io/en/stable/userguide/index.html).

## Additional Performance Considerations

In addition to Parsl, a number of other elements may impact the performance you find with CytoTable.
These are presented in no particular order in terms of impact (this will depend largely on the data sources and system resources available which have a wide variation).

- __Cloud-based vs local data sources__: CytoTable provides source data capabilities through [cloudpathlib](https://cloudpathlib.drivendata.org/stable/), a package for interacting with cloud data storage through a unified API. Cloud-based data sources will generally be processed more slowly than locally available data in CytoTable.
- __Data chunk sizes__: CytoTable uses the `chunk_size` parameter to create row-wise "chunks" of data operations which limits the total amount of memory used by procedures ([see here for more information](overview.md#data-chunking)). Larger chunk sizes can sometimes lead to faster time performance and larger memory footprints. Smaller chunk sizes can lead to slower time performance and smaller memory footprints.
- __PyArrow settings__: CytoTable makes use of [PyArrow](https://arrow.apache.org/docs/python/index.html) for core in-memory data work for performance and integrative capabilities.
    - PyArrow provides the ability to use non-default memory allocation which can sometimes enable greater performance. CytoTable uses the default memory allocation selection performed by PyArrow. One may use the [`ARROW_DEFAULT_MEMORY_POOL`](https://arrow.apache.org/docs/cpp/env_vars.html#envvar-ARROW_DEFAULT_MEMORY_POOL) environment variable to specify which memory allocator is used by CytoTable (`jemalloc`, `mimalloc`, or `(C)malloc`)([see here for more](architecture.technical.md#arrow-memory-allocator-selection)).
    - PyArrow operations through CytoTable use memory mapping (for example [`parquet.read_table(memory_mapped=...)`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html)) and may be turned off through the `CYTOTABLE_ARROW_USE_MEMORY_MAPPING` environment variable ([see here for more](architecture.technical.md#arrow-memory-mapping-selection)).
- __DuckDB settings__: CytoTable uses [DuckDB](https://duckdb.org/docs/) to perform SQL-based data processing. DuckDB provides internal concurrency through the use of threads ([see here for more](https://duckdb.org/docs/connect/concurrency.html)). The number of threads defaults to the number of processors available on the system. The number of threads used by DuckDB may be explicitly set through the `CYTOTABLE_MAX_THREADS` environment variable. Please note: DuckDB is not used to write data in parallel through CytoTable.

+++

## Definitions

We provide the following definitions to help clarify this content.

### Concurrency and Parallelism
- [Concurrency](https://en.wikipedia.org/wiki/Concurrency_(computer_science)): the structure of computer program which allows for non-sequential execution without affecting the outcome.
- [Parallelism](https://en.wikipedia.org/wiki/Parallel_computing): a type of computation in which more than one calculation may take place at the same time.

### Processors and Threads

- [Processor](https://en.wikipedia.org/wiki/Central_processing_unit): a computer resource used to execute instructions from computer software. Computers may have one or many processors.
- [Thread](https://en.wikipedia.org/wiki/Thread_(computing)): a sequence of computer software instructions executed by a processor. A processor may have one or many threads. A processor will only make progress one thread at a time.
- [Multiprocessing](https://en.wikipedia.org/wiki/Multiprocessing): the use of more than one processor to accomplish a software task.
- [Multithreading](https://en.wikipedia.org/wiki/Multithreading_(computer_architecture)): the use of more than one thread to accomplish a software task.

### Parsl

- [Parsl Executors](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.base.ParslExecutor.html): abstractions which represent computer resources available to accomplish tasks through Parsl.
- [`parsl.executors.ThreadPoolExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html): a  Parsl executor with multithreading capabilities.
- [`parsl.executors.HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html): a Parsl executor with multiprocessing capabilities.

+++

### Heuristics

Configuration for Parsl
