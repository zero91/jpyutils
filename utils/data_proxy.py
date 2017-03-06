# coding: utf-8
# Author: Donald Cheung <jianzhang9102@gmail.com>
"""Utility for manipulating data.
"""
import operator

class SchemaWrapper(object):
    """Accessing data from pre-set schema.
    """
    def __init__(self, schema, fixed=True):
        """
        Parameters
        ---------
        schema: basestring/dict/list/tuple/set
            schema of target data.

        fixed: boolean, default False (optional)
            True if target data's length should be the same as schema, otherwise set False.

        """
        if isinstance(schema, basestring):
            self.__schema = dict([(col.strip(), i) for i, col in enumerate(schema.split(','))])
            self.__schema_list = sorted(self.__schema.keys(), key=self.__schema.get)
        elif isinstance(schema, dict):
            self.__schema = schema
            self.__schema_list = self.__schema.keys()
        elif isinstance(schema, (list, tuple, set)):
            column_list = list()
            for idx, column in enumerate(schema):
                if isinstance(column, basestring):
                    column_list.append((column, idx))
                elif isinstance(column, (list, tuple)) and len(column) == 2:
                    column_list.append(column)
                else:
                    raise ValueError("Unsupported schema, item = [{0}]".format(column))
            self.__schema = dict(column_list)
            self.__schema_list = map(operator.itemgetter(0), column_list)
        else:
            raise TypeError("Unsupported schema type")

        if len(set(self.__schema.values())) != len(self.__schema):
            raise ValueError("schema columns dost not set correctly")
        self.__fixed = fixed

    def set_default_columns(self, cols):
        """Set default columns for method `self.get'.

        Parameters
        ---------
        cols: string/list/tuple/set/dict
            columns for default accessing.

        """
        if isinstance(cols, basestring):
            column_list = map(str.strip, cols.split(','))
        elif isinstance(cols, (list, tuple, set, dict)):
            column_list = list(cols)
        else:
            raise TypeError("Unsupported type cols: [{0}]".format(cols))
        self.__schema_list = column_list

    def get(self, data, cols=None):
        """Get columns values from data.

        Parameters
        ----------
        data: list/tuple
            Data from which we accessed.

        cols: string/list/tuple/set/dict
            Columns to get. If None, get default column list.

        Returns
        -------
        column_values: list
            Columns values.
            
        """
        if cols is None:
            cols = self.__schema_list

        if isinstance(cols, basestring):
            column_list = map(str.strip, cols.split(','))
        elif isinstance(cols, (list, tuple, set, dict)):
            column_list = list(cols)
        else:
            raise TypeError("Unsupported type cols: [{0}]".format(cols))

        mismatch_keys = list()
        for col in column_list:
            if col not in self.__schema:
                mismatch_keys.append(col)
        if len(mismatch_keys) > 0:
            raise ValueError("Can't find cols: [{0}]".format(",".join(mismatch_keys)))

        if self.__fixed and len(self.__schema) != len(data):
            raise ValueError("data length does not match schema length")

        col_idx = map(self.__schema.get, column_list)
        if max(col_idx) >= len(data):
            raise ValueError("data length is smaller than columns length to get")

        values = map(lambda c: data[self.__schema[c]], column_list)
        return values
