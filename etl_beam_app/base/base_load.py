from apache_beam import PTransform
from apache_beam.io import WriteToBigQuery
from apache_beam.io import BigQueryDisposition


class LoadBigquery(PTransform):
    def __init__(
        self,
        output_table,
        schema,
        additional_bq_params,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    ):
        self.output_table = output_table
        self.schema = schema
        self.additional_bq_params = additional_bq_params
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition

    def expand(self, pcoll):
        pcoll | WriteToBigQuery(  # type: ignore
            self.output_table,
            schema=self.schema,
            create_disposition=self.create_disposition,
            write_disposition=self.write_disposition,
            additional_bq_parameters=self.additional_bq_params,
        )
