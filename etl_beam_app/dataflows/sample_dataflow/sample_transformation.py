from etl_beam_app.base.base_transform import BaseTransform


class SampleTransformation(BaseTransform):
    def transform(self, data_line):
        data_line["favourite_numbers"] = ",".join(
            str(num) for num in data_line["favourite_numbers"]
        )
        return data_line
