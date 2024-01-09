"""
Attempting to create synthetic datasets from provided collection of CSV's.
"""
import pathlib
from typing import Dict, List, Union

import duckdb
import pyarrow as pa
from faker import Faker

# set a path for local data source
data_dir = "data"

# build a collection of schema
schema_collection: List[Dict[str, Union[str, pa.Schema]]] = []
for data_file in pathlib.Path(data_dir).rglob("*.csv"):
    with duckdb.connect() as ddb:
        # read the csv file as a pyarrow table and extract detected schema
        table = ddb.execute(
            f"""
            SELECT *
            FROM read_csv_auto('{data_file}')
            """
        ).arrow()
        schema_collection.append({"file": data_file, "schema": table.schema})

# determine if the schema are exactly alike
for schema in schema_collection:
    for schema_to_compare in schema_collection:
        # compare every schema to all others
        if schema["file"] != schema_to_compare["file"]:
            if not schema["schema"].equals(schema_to_compare["schema"]):
                raise Exception("Inequal schema detected.")


def find_totally_unique_and_identical_cols(table1, table2):
    unique_cols = []
    identical_cols = []

    # Iterate over columns and rows to compare values
    for column in table1.column_names:
        # Check for identical values
        if (
            table1[column]
            .sort(order="ascending")
            .equals(table2[column].sort(order="ascending"))
        ):
            identical_cols.append(column)
        else:
            unique_cols.append(column)

    return unique_cols, identical_cols


def find_equal_unique_value_cols(table1, table2):
    unique_cols = []
    identical_cols = []

    # Iterate over columns and rows to compare values
    for column in table1.column_names:
        # Check for identical values
        if (
            table1[column]
            .unique()
            .sort(order="ascending")
            .equals(table2[column].unique().sort(order="ascending"))
        ):
            identical_cols.append(column)
        else:
            unique_cols.append(column)

    return unique_cols, identical_cols


tables = []
for data_file in list(pathlib.Path(data_dir).rglob("*.csv"))[:2]:
    with duckdb.connect() as ddb:
        # read the csv file as a pyarrow table append to list for later use
        tables.append(
            ddb.execute(
                f"""
                SELECT *
                FROM read_csv_auto('{data_file}')
                """
            ).arrow()
        )
print(len(tables))

unique_cols, identical_cols = find_totally_unique_and_identical_cols(
    tables[0], tables[1]
)
print(f"Unique columns: {unique_cols}")
print(f"Identical columns: {identical_cols}")
if identical_cols:
    raise Exception(
        "Detected identical columns. Modifications required beyond this point."
    )


# +
# Function to generate fake data using Faker based on column types
def generate_fake_data(column_types, num_rows=10):
    fake = Faker()
    fake_data = {column: [] for column in column_types}

    for _ in range(num_rows):
        for column, data_type in column_types.items():
            if pa.types.is_string(data_type):
                fake_data[column].append(fake.pystr(max_chars=1))
            elif pa.types.is_integer(data_type):
                fake_data[column].append(fake.random_int())
            elif pa.types.is_floating(data_type):
                fake_data[column].append(fake.random_number())
            else:
                # Handle additional data types as needed
                fake_data[column].append(None)

    return fake_data


# Function to create a new PyArrow table with fake data
def create_fake_table(original_schema, num_rows=10):
    column_types = {field.name: field.type for field in original_schema}
    fake_data = generate_fake_data(column_types, num_rows)

    # Create a new schema based on the detected column types
    new_schema = pa.schema(
        [(name, data_type) for name, data_type in column_types.items()]
    )

    # Create a PyArrow table with fake data
    new_table = pa.table(fake_data, schema=new_schema)

    return new_table


# Create a new table with fake data
fake_table = create_fake_table(schema_collection[0]["schema"], num_rows=10)

print("\nNew Table with Fake Data:")
print(fake_table)
