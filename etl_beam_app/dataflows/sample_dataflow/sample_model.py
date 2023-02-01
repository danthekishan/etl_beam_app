"""
data model to store person data

# sample data set
# {"first_name": "xxx", "last_name": "xxx",
 "address": {"street": "123 Main St.", "country": "USA", "zipcode": "97201"},
 "favourite_numbers": []}
"""

from typing import Optional
from etl_beam_app.base.base_model import (
    BaseDataModel,
    ChildDataModel,
    ErrorModel,
    GCPModel,
)
from pydantic import Field
from google.cloud.bigquery import SchemaField


class Address(ChildDataModel):
    street: str
    country: str = "USA"
    zipcode: str


class Person(BaseDataModel):
    first_name: str
    last_name: Optional[str]
    address: Optional[Address] = Field(flatten=True)
    favourite_numbers: list[int]
    filename: str


class PersonGCPModel(GCPModel):
    table_schema = [
        SchemaField("first_name", "STRING"),
        SchemaField("last_name", "STRING"),
        SchemaField("address_street", "STRING"),
        SchemaField("address_country", "STRING"),
        SchemaField("address_zipcode", "STRING"),
        SchemaField("favourite_numbers", "STRING"),
        SchemaField("filename", "STRING"),
        SchemaField("source", "STRING"),
        SchemaField("imported_at", "DATETIME"),
    ]

    def __init__(self, project_id):
        super().__init__(
            project_id=project_id,
            dataset_id="trips_data_all",
            table_id="person",
            table_schema=PersonGCPModel.table_schema,
        )
