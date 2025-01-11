# -*- encoding: utf-8 -*-

"""
Read Raw Data File(s) Typically Downloaded from TradingView
"""

from typing import Iterable
from abc import ABC, abstractmethod

import pandas as pd

class BaseReader(ABC):
    def __init__(self, filepath : str, timestamp : str = "ISO") -> None:
        self.filepath = filepath
        self.timestamp = self._assertvalues(timestamp, ["ISO", "UNIX"])


    @abstractmethod
    def read_file(self, filetype : str, **kwargs) -> pd.DataFrame:
        """
        Read the File and Return as a DataFrame

        The method reads the file and returns the data as a
        :class:`pandas.DataFrame`. The method is abstract and is
        to be implemented by the child class.
        """
        
        pass


    @staticmethod
    def _assertvalues(value : object, allowed : Iterable[object]) -> object:
        """
        Static Class Method to Assert Values

        Allowed values are passed as a list and the value is checked,
        if fails raises an assertion error, else returns the value.
        """

        assert value in allowed, \
            f"Invalid Value. Got {value} Allowed: {allowed}"
        return value


    @staticmethod
    def _assertdtype(value : object, allowed : Iterable[type]) -> object:
        """
        Static Class Method to Assert Data Type

        The method is used to assert the data type of the value passed.
        If the data type is not as expected, then it raises an assertion
        error, else returns the value.
        """

        assert type(value) in allowed, \
            f"Invalid Data Type. Got {type(value)} Expected: {allowed}"
        return value


    @staticmethod
    def __selfkwargs__() -> list:
        """
        Return(s) a List of Keyword Arguments for the Class

        The keyword arguments are returned as a list of strings, which
        should be ignored when calling :mod:`pandas` functions. Any
        defined keyword argument not in the list is passed to the
        :func:`pd.read_*()` function.

        ..warning::

            This method is not a good programming practice, but is
            defined to keep development simple.
        """

        return [
            # read_file()
            "timeperiod",

            # parse_dates()
            "dtformat",
            "keepdatepart"
        ]


class DataReader(BaseReader):
    def __init__(self, filepath : str, timestamp : str = "ISO") -> None:
        super().__init__(filepath = filepath, timestamp = timestamp)


    def read_file(
        self,
        filetype : str = "csv",
        dtcolumns : list = [],
        todatetime : bool = False,
        **kwargs
    ) -> pd.DataFrame:
        timeperiod = kwargs.get("timeperiod", "infer")
        cfunc = dict(csv = pd.read_csv, xlsx = pd.read_excel)

        # seperate out possible pandas argument external controls
        pdkwargs = {
            k : v for k, v in kwargs.items()
            if k not in self.__selfkwargs__()
        }

        frame = cfunc[filetype](self.filepath, **pdkwargs)

        return self.parse_dates(
            frame,
            dtcolumns = dtcolumns,
            todatetime = todatetime,
            **kwargs
        )


    def parse_dates(
        self,
        frame : pd.DataFrame,
        dtcolumns : list,
        todatetime : bool,
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

        :type  dtcolumns: list
        :param dtcolumns: List of columns where parsing is done
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

        dtformat = self._assertdtype(
            kwargs.get("dtformat", "%Y-%m-%dT%H:%M:%S+05:30"),
            allowed = [str, dict]
        )
        keepdatepart = self._assertdtype(
            kwargs.get("keepdatepart", False),
            allowed = [bool, dict]
        )

        # if assertion is not failed, then we can convert the data to dict if not already
        dtformat = dtformat if type(dtformat) == dict else { c : dtformat for c in dtcolumns }
        keepdatepart = keepdatepart if type(keepdatepart) == dict else { c : keepdatepart for c in dtcolumns }

        frame = frame.copy()
        for column in dtcolumns:
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
