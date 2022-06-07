import pandas as pd


class ExcelWriter:
    def __init__(self, file=None):
        self._file = file

    