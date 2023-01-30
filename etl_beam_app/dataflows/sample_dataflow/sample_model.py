"""
data model to store person data

# sample data set
# {"first_name": "xxx", "last_name": "xxx",
 "address": {"street": "123 Main St.", "country": "USA", "zipcode": "97201"},
 "favourite_numbers": []}
"""

from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel, ErrorModel
from pydantic import Field


class Address(ChildDataModel):
    street: str
    country: str = "USA"
    zipcode: str


class Person(BaseDataModel):
    first_name: str
    last_name: str | None
    address: Address | None = Field(flatten=True)
    favourite_numbers: list[int]
    filename: str


class JsonError(ErrorModel):
    pass
