import logging
import argparse
import apache_beam as beam

from etl_beam_app.base.base_extract import ReadJsonFiles
from etl_beam_app.connectors.simple_connector import SimpleConnector
from etl_beam_app.dataflows.sample_dataflow.sample_model import JsonError, Person


def run(input_file: str, output_table: str, beam_args: list[str]):
    with beam.Pipeline() as p:
        line = p | ReadJsonFiles(input_file)
        output = line | beam.ParDo(
            SimpleConnector(data_model=Person, error_model=JsonError)
        ).with_outputs(
            SimpleConnector.output_tag_data, SimpleConnector.output_tag_error
        )
        output[SimpleConnector.output_tag_data] | beam.Map(print)  # type: ignore
        # edited = line | beam.Map(lambda line: json.load(line))
        # edited | beam.Map(print)  # type: ignore


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", help="json file of person data")
    parser.add_argument("--output_table", help="bigquery table name")
    args, beam_args = parser.parse_known_args()
    run(input_file=args.input_file, output_table=args.output_table, beam_args=beam_args)
