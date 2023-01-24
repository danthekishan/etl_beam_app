from typing import Generic
from typing import TypeVar

from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel, ErrorModel

from pydantic.generics import GenericModel
from pydantic import validator


_DataModel = TypeVar("_DataModel")
_ErrorModel = TypeVar("_ErrorModel")


class BaseExtractResponse(GenericModel, Generic[_DataModel, _ErrorModel]):
    """
    Base the extracted response.
    It can be either a DataModel or ErrorModel
    """

    data_model: BaseDataModel | ChildDataModel | None
    error_model: ErrorModel | None

    @validator("error_model", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data_model"] is not None:
            raise ValueError("Must not provide both data and error")
        if v is None and values["data_model"] is None:
            raise ValueError("Must provide data or error")
        return v
