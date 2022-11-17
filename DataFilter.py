from abc import ABCMeta

import pandas as pd

SINGLE_ELEMENT = 1


@pd.api.extensions.register_dataframe_accessor('dfilter')
class DataFilter(metaclass=ABCMeta):
    # instantiate class
    def __init__(self, pandas_obj):
        self._df = pd.DataFrame(pandas_obj)
        self._headers = self._df.keys()

    def keep_rows(self, col, rows):
        # string for multiple string removal
        all_strings = ''
        # instantiate as a list
        if not isinstance(rows, list):
            rows = list(rows)
        # if just a single string
        if len(rows) == SINGLE_ELEMENT:
            # do normal removal
            self._df[col] = self._df[col].map(str)
            self._df = self._df[self._df[col].str.contains(rows[0])]
        # if removing more than one string
        elif len(rows) > SINGLE_ELEMENT:
            # loop through each string and create a list string of all unwanted values
            for st in rows:
                all_strings += st + '|'
            # remove last | from the string
            all_strings = all_strings[:-1]
            # apply the super string and remove all the row values
            self._df[col] = self._df[col].map(str)
            self._df = self._df[self._df[col].str.contains(all_strings)]
        return self._df

    def remove_rows(self, col, rows):
        # converts rows to list for iteration
        if not isinstance(rows, list):
            rows = list(rows)
        # for each element in the list
        for element in rows:
            # checks whether it's an index (int) or a string variable
            if isinstance(element, int):
                # just drops the selected index from the table
                self._df = self._df.drop(element, axis=0)
            # if it's a string
            elif isinstance(element, str):
                # drops all related string elements from within the dataframe
                # maps all elements within the check col to str
                self._df[col] = self._df[col].map(str)
                # slices dataframe
                self._df = self._df[~self._df[col].str.contains(element)]
        # return statement
        return self._df

    def keep_col(self, final_col):
        # subtracts the keep col from the headers to drop
        return self._df.drop(list(set(self._headers) - set(final_col)), axis=1).reset_index(drop=True)

    def remove_col(self, final_col):
        # just removes the columns you don't want
        return self._df.drop(list(final_col), axis=1)
