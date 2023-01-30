"""
Defining beam extracting PTransforms, and the supporting DoFn
"""
import json
from apache_beam import DoFn, PTransform, Reshuffle, Map, ParDo

from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io import ReadAllFromText


class ParseJson(DoFn):
    """
    Parsing json records as dict. json file should
    have each json record in each line, and should not
    have list or seperated by comma
    """

    def process(self, elem):
        """
        returing parsed data in dict along with file name
        """
        filename, line = elem
        data = json.loads(line)
        data["filename"] = filename
        yield data


class ReadableFiles(PTransform):
    """
    Getting list of files according to the pattern and
    returning sequence of files
    """

    def __init__(
        self, match_files: str, compression_type: CompressionTypes | None = None
    ):
        """
        init

        args:
            match_files - pattern matching files
            compression_type - compressed type
        """
        self.match_files = match_files
        self.compression_type = compression_type

    def expand(self, pcoll):
        """
        Matching files and returing sequence of matched files
        """
        readable_files = (
            pcoll
            | "Matching files" >> MatchFiles(self.match_files)
            | "Reading matched" >> ReadMatches(self.compression_type)
            | Reshuffle()
        )

        return readable_files | "extract path" >> Map(lambda path: path.metadata.path)


class ReadTextFiles(PTransform):
    """
    Reading text files
    """

    def __init__(
        self, match_files, skip_headers=1, filename=True, compression_type=None
    ):
        """
        init

        args:
            match_files - pattern for matching files
            skip_headers - skip headers in numbers
            filename - if filename needed in output
            compression_type - compression type of file
        """
        self.match_files = match_files
        self.skip_headers = skip_headers
        self.filename = filename
        self.compression_type = compression_type

    def expand(self, pcoll):
        """return a sequence of data, read from file"""
        return (
            pcoll
            | "Readable files" >> ReadableFiles(self.match_files, self.compression_type)
            | "Read text files"
            >> ReadAllFromText(
                skip_header_lines=self.skip_headers, with_filename=self.filename
            )
        )


class ReadJsonFiles(PTransform):
    """
    Reading json files
    """

    def __init__(self, match_files, filename=True, compression_type=None):
        """
        init

        args:
            match_files - pattern for matching files
            filename - if filename needed in output
            compression_type - compression type of file
        """
        self.match_files = match_files
        self.filename = filename
        self.compression_type = compression_type
        self.skip_headers = 0

    def expand(self, pcoll):
        """return a sequence of dict, read from file"""
        return (
            pcoll
            | "Read file"
            >> ReadTextFiles(
                self.match_files,
                self.skip_headers,
                self.filename,
                self.compression_type,
            )
            | "Parse json" >> ParDo(ParseJson())
        )
