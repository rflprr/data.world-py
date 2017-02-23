import csv

from io import StringIO


class Results:
    def __init__(self, raw):
        self.raw = raw

    def __unicode__(self):
        return self.as_string()

    def __repr__(self):
        return "{0}\n...".format(self.as_string()[:250])

    def as_string(self):
        return self.raw

    def as_stream(self):
        return StringIO(self.raw)

    def as_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("You need to have pandas installed to use .asDf()")
        else:
            return pd.read_csv(self.as_stream())

    def as_csv(self):
        # TODO: support UTF-8 formatted CSV in Python 2.x
        return csv.reader(self.as_stream())
