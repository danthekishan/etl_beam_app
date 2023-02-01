from datetime import datetime
import json
from typing import Generic, Optional
from typing import TypeVar

from pydantic import validator
from pydantic.generics import GenericModel

from etl_beam_app.base.base_model import BaseDataModel, ErrorModel
from etl_beam_app.base.base_transform import BaseTransform


_DataModel = TypeVar("_DataModel")
_ErrorModel = TypeVar("_ErrorModel")


class BaseExtractResponse(GenericModel, Generic[_DataModel, _ErrorModel]):
    """
    Generic model that handles the extracted data or errors, if occured
    """

    data_model: Optional[BaseDataModel]
    error_model: Optional[ErrorModel]

    @validator("error_model", always=True)
    def check_consistency(cls, v, values):
        """
        checking the whether error data or data only available at a time
        """
        if v is not None and values["data_model"] is not None:
            raise ValueError("Must not provide both data and error")
        if v is None and values["data_model"] is None:
            raise ValueError("Must provide data or error")
        return v


class BaseET:
    """
    A class that represent the Extract, Transform data
    """

    def __init__(
        self,
        data_model,
        error_model,
        source: str,
        transform_class: Optional[BaseTransform] = None,
        extract_response=BaseExtractResponse,
    ):
        self.data_model = data_model
        self.error_model = error_model
        self.source = source
        self.imported_at = datetime.utcnow()
        self.transform_class = (
            transform_class() if transform_class is not None else None  # type: ignore
        )
        self.extract_response = extract_response

    def validate_data(self, data: dict) -> tuple[dict, dict]:
        """
        Validating data using pydantic backend

        args:
            data - input data

        return:
            output_data - pydantic data class based data dict
            output_error - pydantic error class based data dict
        """
        output_data, output_error = {}, {}
        try:
            output_data = self.extract_response(
                data_model=self.data_model(
                    **data, source=self.source, imported_at=self.imported_at
                ),
                error_model=None,
            ).dict()["data_model"]
        except Exception as e:
            message = "test_message"
            output_error = self.extract_response(
                data_model=None,
                error_model=self.error_model(
                    exception=e.__repr__(),
                    message=message,
                    raw_data=json.dumps(data, default=str),
                    error_at=self.imported_at,
                ),
            ).dict()["error_model"]
        return output_data, output_error

    def process_data(self, data: dict) -> tuple[dict, dict]:
        """
        Processing and transforming single data point

        args:
            data

        return:
            data
            error
        """
        data, error = self.validate_data(data)

        if self.transform_class is not None:
            data = self.transform_class.transform(data_line=data)

        return data, error
