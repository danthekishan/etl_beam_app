"""
base_model holds pydantic classes and support enum classes
that can be extended to create customized classes.
"""
from datetime import datetime
from typing import Optional
from apache_beam.io.gcp.internal.clients import bigquery
from pydantic import BaseModel
from google.cloud.bigquery import SchemaField


class DataModel(BaseModel):
    """
    Data Model class that handles the nested schema and
    outputs a flattened dict obj, only when the nested
    model has `Field(flatten=True)` when defining the model
    """

    def _iter(self, to_dict: bool = False, *args, **kwargs):
        for dict_key, value in super()._iter(to_dict, *args, **kwargs):
            if to_dict and self.__fields__[dict_key].field_info.extra.get(
                "flatten", False
            ):
                assert isinstance(value, dict)
                yield from (
                    (f"{dict_key}_{key}", value) for key, value in value.items()
                )
            else:
                yield dict_key, value


class BaseDataModel(DataModel):
    """
    Base data model class with essential fields
    """

    source: str
    imported_at: datetime


class ChildDataModel(DataModel):
    """
    Child data model class without any data model
    """

    pass


class ErrorModel(BaseModel):
    """
    Basic error model with essential fields
    """

    exception: str
    message: str
    raw_data: str
    error_at: datetime


class JsonError(ErrorModel):
    pass


class GCPModel:
    """
    This class stores the additional information
    about the gcp and related configurations
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        table_schema: list[SchemaField],
        additional_bq_parameters: Optional[dict] = {},
    ):
        """
        __init__

        args:
            project_id - gcp project id
            dataset_id - bigquery dataset
            table_id - bigquery table
            table_schema - list of dict/schema
                ex:
                    [{'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'}]
            additional_bq_parameters - bigquery additional params
                ex:
                    {'timePartitioning': {'type': 'HOUR'}}
            write_disposition - bigquery writing options(default=append)
            create_disposition - bigquery create options(default=create)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_schema = {
            "fields": [record.to_api_repr() for record in table_schema]
        }
        self.additional_bq_parameters = (
            additional_bq_parameters if additional_bq_parameters == {} else None
        )
        self.table_spec = bigquery.TableReference(
            projectId=self.project_id, datasetId=self.dataset_id, tableId=self.table_id
        )
