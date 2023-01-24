import json
from pprint import pprint

from etl_beam_app.etl.extract import ETL
from etl_beam_app.models.person import Person, JsonError


with open("etl_beam_app/data/input_data.json") as file:
    data = json.load(file)

people_datapipeline = ETL(Person, JsonError)
people_datapipeline.extract_bulk_data(source="input_test", data=data)
print(people_datapipeline.extracted_data)
print()
print(people_datapipeline.extracted_errors)

# people = [
#     ExtractResponse(
#         data_model=Person(**d, source="test_source"), error_model=None
#     ).dict()
#     for d in data
# ]
# print(people)  # schema of the model
"""
Plan

1. process data -> output data and error datasets :: =DONE=
2. push data to data warehouse and cloud storage
3. push errors to cloud storage
4. develop pipeline to process error storage
    1. clean and produce proper data
    2. processed data to dw and cloud storage
    3. errors data file to be deleted or archieved
5. notification integration
    1. email
    2. slack
6. archieve cloud storage blobs (once a year)
7. backfill data pipelines
"""
