"""
base_model holds pydantic classes and support enum classes
that can be extended to create customized classes.
"""
from datetime import datetime
from pydantic import BaseModel


class DataModel(BaseModel):
    """
    Data Model class that handles the nested schema and
    outputs a flattened dict obj, only when the nested
    model has `Field(flatten=True)` when defining the model
    """

    def _iter(self, to_dict: bool = False, *args, **kwargs):
        for dict_key, value in super()._iter(to_dict, *args, **kwargs):
            if to_dict and self.__fields__[dict_key].field_info.extra.get(
                "flatten", False
            ):
                assert isinstance(value, dict)
                yield from (
                    (f"{dict_key}_{key}", value) for key, value in value.items()
                )
            else:
                yield dict_key, value


class BaseDataModel(DataModel):
    """
    Base data model class with essential fields
    """

    source: str
    imported_at: datetime


class ChildDataModel(DataModel):
    """
    Child data model class without any data model
    """

    pass


class ErrorModel(BaseModel):
    """
    Basic error model with essential fields
    """

    exception: str
    message: str
    raw_data: str
    error_at: datetime
