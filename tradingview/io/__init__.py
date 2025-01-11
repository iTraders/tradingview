# -*- encoding: utf-8 -*-

"""
The Submodule to Deal with Input-Output Operations

As of now, there are no official source of API from TradingView and
thus we will have to rely on methods like web-scrapping and/or manual
data download using the "Export Data" option in TradingView.

The submodule are as below - :mod:`aided` and :mod:`automatic`. The
module :mod:`aided` is for manual data download and processing is done
in steps (wip), while the module :mod:`automatic` is planned for the
future to automate the process of data download and processing.
"""

from tradingview.io.aided import * # noqa: F401, F403 # pyright: ignore[reportMissingImports]
