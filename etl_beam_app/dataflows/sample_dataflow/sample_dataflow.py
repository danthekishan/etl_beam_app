import logging
import argparse
from typing import List
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions

from etl_beam_app.base.base_extract import ReadJsonFiles
from etl_beam_app.base.base_load import LoadBigquery
from etl_beam_app.base.base_model import JsonError
from etl_beam_app.connectors.simple_connector import SimpleConnector
from etl_beam_app.dataflows.sample_dataflow.sample_model import Person, PersonGCPModel
from etl_beam_app.dataflows.sample_dataflow.sample_transformation import (
    SampleTransformation,
)


def run(input_file: str, output_file: str, beam_args: List[str]):

    # support model
    person_gcp = PersonGCPModel(project_id="learn-de-370612")
    table_spec = person_gcp.table_spec
    bq_schema = person_gcp.table_schema
    bq_params = person_gcp.additional_bq_parameters

    # pipeline configuration
    options = PipelineOptions(beam_args, save_main_session=True)

    # pipeline
    with beam.Pipeline(options=options) as p:
        # with beam.Pipeline() as p:
        line = p | ReadJsonFiles(input_file)
        output = line | beam.ParDo(
            SimpleConnector(
                data_model=Person,
                error_model=JsonError,
                source="Json file",
                transform_class=SampleTransformation,
            )
        ).with_outputs(
            SimpleConnector.output_tag_data, SimpleConnector.output_tag_error
        )
        # output[SimpleConnector.output_tag_data] | beam.Map(print)  # type: ignore
        if not output_file:
            output[SimpleConnector.output_tag_data] | LoadBigquery(table_spec, bq_schema, bq_params)  # type: ignore
        else:
            output[SimpleConnector.output_tag_data] | WriteToText(output_file)  # type: ignore
        # edited = line | beam.Map(lambda line: json.load(line))
        # edited | beam.Map(print)  # type: ignore


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", help="json file of person data")
    parser.add_argument("--output_file", help="bigquery table name")
    args, beam_args = parser.parse_known_args()
    run(input_file=args.input_file, output_file=args.output_file, beam_args=beam_args)
