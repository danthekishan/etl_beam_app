# from etl_beam_app.etl.person_etl import person_et
#
#
# person_et.process_data()
# df = person_et.data_to_df()
# print(df)
"""
Plan
#####
Create a new support model that would have,
for api -->
    * api endpoint
    * api endpoint parameters
    * bigquery schema
create a switcher, that can switch between,
    * data, error model
    * support model
and should be PTransform
#####

1. process data output data and error datasets -> ::DONE:: | test -> done
* flatten nested data -> ::DONE:: | test -> done
* build connectors
* develop for beam ====
    * model validator -> DONE
    * data extracting (connector) -> DONE
    * data loading (template) -> PENDING
* setup file instead of pyproject.toml


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
