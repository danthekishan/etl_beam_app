import pandas as pd


class Load:
    @classmethod
    def to_df(cls, output):
        return pd.DataFrame.from_records(output)
