from apache_beam import TaggedOutput
from etl_beam_app.base.base_connector import BaseConnector


class SimpleConnector(BaseConnector):
    def __init__(self, data_model, error_model, source="Simple file"):
        super().__init__(data_model, error_model, source)

    def setup(self):
        self.et = self.setup_et()

    def process(self, element):
        data, error = self.et.process_data(data=element)
        if data:
            yield TaggedOutput(SimpleConnector.output_tag_data, data)
        else:
            yield TaggedOutput(SimpleConnector.output_tag_error, error)
