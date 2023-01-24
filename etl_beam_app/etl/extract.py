import json
from etl_beam_app.base.base_response import BaseExtractResponse


class ExtractResponse(BaseExtractResponse):
    pass


class ETL:
    def __init__(self, data_model, error_model):
        self.data_model = data_model
        self.error_model = error_model
        self.extracted_data = []
        self.extracted_errors = []

    def extract_data(self, source: str, **data: dict) -> tuple[dict, dict]:
        output_data, output_error = {}, {}
        try:
            output_data = ExtractResponse(
                data_model=self.data_model(source=source, **data),
                error_model=None,
            ).dict()
        except Exception as e:
            message = "test_message"
            output_error = ExtractResponse(
                data_model=None,
                error_model=self.error_model(
                    exception=e.__repr__(), message=message, raw_data=json.dumps(data)
                ),
            ).dict()
        return output_data, output_error

    def extract_bulk_data(self, source: str, data: list):
        for record in data:
            output_data, output_error = self.extract_data(source, **record)
            if output_data:
                self.extracted_data.append(output_data)

            if output_error:
                self.extracted_errors.append(output_error)
