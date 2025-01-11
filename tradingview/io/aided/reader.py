# -*- encoding: utf-8 -*-

"""
Read Raw Data File(s) Typically Downloaded from TradingView
"""

import pandas as pd

class DataReader:
    def __init__(self, filepath : str, timestamp : str = "ISO") -> None:
        self.filepath = filepath
        self.timestamp = timestamp # ? ISO/UNIX Format Download


    def read_file(self, filetype : str = "csv", **kwargs) -> pd.DataFrame:
        timeperiod = kwargs.get("timeperiod", "infer")

        cfunc = dict(csv = pd.read_csv, xlsx = pd.read_excel)
        frame = cfunc[filetype](self.filepath, **kwargs)
        return self.parse_dates(frame, **kwargs)


    def parse_dates(
        self,
        frame : pd.DataFrame,
        columns : list,
        todatetime : bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """
        Parse Date Columns of the Data using Methods

        The date column is parsed using :func:`pd.to_datetime()`
        method, as the use of :attr:`parse_dates` (and other related
        arguments) are being deprecated by the :mod:`pandas`. The
        function also offers the functionality to convert the
        :func:`pd.Timestamp` to a :attr:`datetime` object and
        additional functionalities like keeping only date part.

        :type  columns: list
        :param columns: List of columns where parsing is done
            iteratively. If passing a single value, then also pass
            the same as a list (todo fix).

        :type  todatetime: bool
        :param todatetime: Convert the :func:`pd.Timestamp` as a
            native :attr:`datetime` object (todo).

        Keyword Arguments
        -----------------

        Please note that all the keyword arguments are desigend as per personal
        requirements, considering the local as India. Any changes or requirements
        are to be routed through issues/pull requests.

            * **dtformat** (*str*, *dict*) - Datetime format, this can either
                be a string, i.e., all the mentioned columns will be infered with
                the same format, or a dictionary of column names and their types.
                Defaults to :attr:`"%Y-%m-%d %H:%M:%S+05:30"` format (todo).

            * **keepdatepart** (*bool*, *dict*) - Keep only date part from the
                date/datetime stamped columns. Again, this can either be a single
                value or each column can be configured seperately. Defaults to
                False, value is kept intact (todo).
        """

        dtformat = kwargs.get("dtformat", "%Y-%m-%d %H:%M:%S+05:30")
        keepdatepart = kwargs.get("keepdatepart", False)

        # consider converting all the dtformat/keepdatepart for each column
        # else, the value must be an instance of dictionary object, or value
        assert type(dtformat) in [str, dict], "Invalid format for `dtformat` argument."
        assert type(keepdatepart) in [bool, dict], "Invalid format for `keepdatepart` argument."

        # if assertion is not failed, then we can convert the data to dict if not already
        dtformat = dtformat if type(dtformat) != dict else { c : dtformat for c in columns }
        keepdatepart = keepdatepart if type(keepdatepart) != dict else { c : keepdatepart for c in columns }

        frame = frame.copy()
        for column in columns:
            frame[column] = pd.to_datetime(frame[column], format = dtformat[column])

        return frame


    def __call__(self, filetype : str = "csv", **kwargs) -> pd.DataFrame:
        return self.read_file(filetype, **kwargs)


def read_data_export(filepath : str, filetype : str = "csv", **kwargs) -> pd.DataFrame:
    """
    Read Data Exported from TradingView
    """

    reader = DataReader(filepath, timestamp = kwargs.get("timestamp", "ISO"))
    return reader(filetype, **kwargs)
