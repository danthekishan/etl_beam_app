import unittest
from datetime import datetime

from pydantic import Field

from etl_beam_app.base.base_etl import BaseExtractResponse
from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel


class BaseResponseTests(unittest.TestCase):
    """
    Test cases for BaseExtractResponse class, testing
    the output
    """

    def setUp(self) -> None:
        self._input = {
            "name": "Sample Book",
            "pages": 100,
            "author": {"name": "Martin"},
            "source": "test_source",
            "imported_at": datetime(2023, 1, 1),
        }

        self._expected = {
            "data_model": {
                "name": "Sample Book",
                "pages": 100,
                "author_name": "Martin",
                "source": "test_source",
                "imported_at": datetime(2023, 1, 1),
            },
            "error_model": None,
        }

    def book_model(self):
        class Author(ChildDataModel):
            name: str

        class Book(BaseDataModel):
            name: str
            pages: int
            author: Author = Field(flatten=True)

        return Book

    def test_extract_data_response(self):
        """
        Testing the output when data model is passed
        """
        Book = self.book_model()
        response = BaseExtractResponse(
            data_model=Book(**self._input), error_model=None
        ).dict()
        assert response == self._expected
