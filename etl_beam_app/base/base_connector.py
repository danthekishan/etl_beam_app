from apache_beam import DoFn
from etl_beam_app.base.base_etl import BaseET, BaseExtractResponse

from etl_beam_app.base.base_model import BaseDataModel, ErrorModel
from etl_beam_app.base.base_transform import BaseTransform


class BaseConnector(DoFn):
    output_tag_data = "data_record"
    output_tag_error = "error_record"

    def __init__(
        self,
        data_model: BaseDataModel,
        error_model: ErrorModel,
        source: str,
        transform_class: BaseTransform | None = None,
        et_class=BaseET,
        extract_response=BaseExtractResponse,
    ):
        self.data_model = data_model
        self.error_model = error_model
        self.source = source
        self.transform_class = transform_class
        self.et_class = et_class
        self.extract_response = extract_response

    def setup_et(self) -> BaseET:
        et = self.et_class(
            data_model=self.data_model,
            error_model=self.error_model,
            source=self.source,
            transform_class=self.transform_class,
            extract_response=self.extract_response,
        )
        return et
