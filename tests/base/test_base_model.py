from datetime import datetime
from pydantic import Field
import unittest
from etl_beam_app.base.base_model import BaseDataModel, ChildDataModel


class BaseModelTests(unittest.TestCase):
    """
    Test case for BaseDataModel and ChildDataModel
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

    def test_flatten_nested_model(self):
        """
        Testing that BaseDataModel outputs a flatten nested model
        """
        Book = self.book_model()
        book = Book(**self._input).dict()
        assert book == self._expected

    # TODO: implement a function for testing nested-nested schema flattening


if __name__ == "__main__":
    unittest.main()
