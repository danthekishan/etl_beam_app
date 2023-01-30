from datetime import datetime
import unittest
from unittest.mock import patch
from typing import Generator

from pydantic import Field
import pandas as pd
from pandas.testing import assert_frame_equal

from etl_beam_app.base.base_etl import BaseET
from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel, ErrorModel


class ETLTests(unittest.TestCase):
    def setUp(self) -> None:

        self._source = "test_source"

        self._input = {
            "name": "Sample Book",
            "pages": 100,
            "author": {"name": "Martin"},
        }

        self._expected = {
            "name": "Sample Book",
            "pages": 100,
            "author_name": "Martin",
            "source": "test_source",
            "imported_at": datetime(2023, 1, 1),
        }

    def book_model(self):
        class Author(ChildDataModel):
            name: str

        class Book(BaseDataModel):
            name: str
            pages: int
            author: Author = Field(flatten=True)

        return Book

    def helper_et(self):
        Book = self.book_model()
        et = BaseET(
            data_model=Book,
            error_model=ErrorModel,
            source="test_source",
        )
        return et

    @patch("etl_beam_app.base.base_etl.datetime")
    def test_validated_data(self, mock_dt):
        mock_dt.utcnow.return_value = datetime(2023, 1, 1)
        Book = self.book_model()
        et = BaseET(data_model=Book, error_model=ErrorModel, source="test_source")
        data, _ = et.process_data(data=self._input)
        assert data == self._expected
