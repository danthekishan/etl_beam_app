from apache_beam import TaggedOutput
from etl_beam_app.base.base_connector import BaseConnector
from etl_beam_app.base.base_etl import BaseET, BaseExtractResponse


class SimpleConnector(BaseConnector):
    def __init__(
        self,
        data_model,
        error_model,
        source,
        transform_class,
        et_class=BaseET,
        extract_response=BaseExtractResponse,
    ):
        super().__init__(
            data_model, error_model, source, transform_class, et_class, extract_response
        )

    def setup(self):
        self.et = self.setup_et()

    def process(self, element):
        data, error = self.et.process_data(data=element)
        if data:
            yield TaggedOutput(SimpleConnector.output_tag_data, data)
        else:
            yield TaggedOutput(SimpleConnector.output_tag_error, error)
