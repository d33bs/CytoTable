"""
pycytominer-transform: convert - transforming CellProfiler data for
by use with pyctyominer.
"""

import pathlib
from typing import Any, Dict, List, Literal, Optional

import pyarrow as pa
from prefect import flow, task
from pyarrow import csv, parquet

DEFAULT_TARGETS = ["image", "cells", "nuclei", "cytoplasm"]


@task
def get_source_filepaths(
    path: str, targets: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """

    Args:
      path: str:
      targets: List[str]:

    Returns:

    """

    records = []
    for file in pathlib.Path(path).glob("**/*"):
        if file.is_file() and (str(file.stem).lower() in targets or targets is None):
            records.append({"source_path": file})

    if len(records) < 1:
        raise Exception(
            f"No input data to process at path: {str(pathlib.Path(path).resolve())}"
        )

    grouped_records = {}
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source] = [
            source for source in records if source["source_path"].name == unique_source
        ]

    return grouped_records


@task
def read_csv(record: Dict[str, Any]) -> Dict[str, Any]:
    """

    Args:
      record: Dict:

    Returns:

    """

    table = csv.read_csv(input_file=record["source_path"])
    record["table"] = table

    return record


@task
def concat_tables(
    records: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """

    Args:
      records: List[Dict[str, Any]]:

    Returns:

    """

    for group in records:
        if len(records[group]) < 2:
            continue
        records[group] = [
            {
                "source_path": pathlib.Path(
                    (
                        f"{records[group][0]['source_path'].parent.parent}"
                        f"/{records[group][0]['source_path'].name}"
                    )
                ),
                "table": pa.concat_tables(
                    [record["table"] for record in records[group]]
                ),
            }
        ]

    return records


@task
def write_parquet(record: Dict, dest_path: str = "", unique_name: bool = False) -> Dict:
    """

    Args:
      record: Dict:
      unique_name: bool:

    Returns:

    """

    pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)

    destination_path = pathlib.Path(
        f"{dest_path}/{str(record['source_path'].stem)}.parquet"
    )

    if unique_name:
        destination_path = pathlib.Path(
            (
                f"{dest_path}/{str(record['source_path'].parent.name)}"
                f".{str(record['source_path'].stem)}.parquet"
            )
        )

    parquet.write_table(table=record["table"], where=destination_path)

    record["destination_path"] = destination_path

    return record


@task
def infer_source_datatype(
    records: Dict[str, List[Dict[str, Any]]], target_datatype: Optional[str] = None
) -> str:
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    suffixes = list(set((group.split(".")[-1]).lower() for group in records))

    if target_datatype is None and len(suffixes) > 1:
        raise Exception(
            f"Detected more than one inferred datatypes from source path: {suffixes}"
        )

    if target_datatype is not None and target_datatype not in suffixes:
        raise Exception(
            (
                f"Unable to find targeted datatype {target_datatype} "
                "within files. Detected datatypes: {suffixes}"
            )
        )

    if target_datatype is None:
        target_datatype = suffixes[0]

    return target_datatype


@flow
def to_arrow(
    path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
):
    """

    Args:
      path: str:
      source_datatype: Optional[str]:  (Default value = None)
      targets: List[str]:  (Default value = None)
      concat: bool: (Default value = True)

    Returns:

    """

    if targets is None:
        targets = DEFAULT_TARGETS

    records = get_source_filepaths(path=path, targets=targets)

    source_datatype = infer_source_datatype(
        records=records, target_datatype=source_datatype
    )

    for group in records:  # pylint: disable=consider-using-dict-items
        if source_datatype == "csv":
            tables_map = read_csv.map(record=records[group])
        records[group] = [table.wait().result() for table in tables_map]

    if concat:
        records = concat_tables(records=records)

    return records


@flow
def to_parquet(records: Dict[str, List[Dict[str, Any]]], dest_path: str = ""):
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    for group in records:
        if len(records[group]) > 2:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=True
            )
        else:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=False
            )

        records[group] = [destination.wait().result() for destination in destinations]

    return records


def convert(
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
):  # pylint: disable=too-many-arguments
    """

    Args:
      path: str:
      source_datatype: str:
      dest_datatype: str:
      targets: List[str]:  (Default value = None):

    Returns:

    """
    if targets is None:
        targets = ["image", "cells", "nuclei", "cytoplasm"]

    records = to_arrow(
        path=source_path,
        source_datatype=source_datatype,
        targets=targets,
        concat=concat,
    )

    if dest_datatype == "parquet":
        output = to_parquet(records=records, dest_path=dest_path)

    return output
