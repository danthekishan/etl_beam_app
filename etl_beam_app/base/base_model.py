"""
base_model holds pydantic classes and support enum classes
that can be extended to create customized classes.
"""
from datetime import datetime
from pydantic import BaseModel


class BaseDataModel(BaseModel):
    """
    Base data model class with essential fields
    """

    source: str
    imported_at: datetime = datetime.utcnow()


class ChildDataModel(BaseModel):
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


# if __name__ == "__main__":
#     data = DataModel(source="test_source")
#     error = ErrorModel(error_type=Errors.invalid_dt, message="test_error")
#
#     print(ExtractResponse(data_model=data, error_model=None))
