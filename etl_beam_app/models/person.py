"""
data model to store person data

# sample data set
# {"first_name": "xxx", "last_name": "xxx",
 "address": {"street": "123 Main St.", "country": "USA", "zipcode": "97201"},
 "favourite_numbers": []}
"""

from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel, ErrorModel


class Address(ChildDataModel):
    street: str
    country: str = "USA"
    zipcode: str


class Person(BaseDataModel):
    first_name: str
    last_name: str | None
    address: Address | None
    favourite_numbers: list[int]


class JsonError(ErrorModel):
    pass
