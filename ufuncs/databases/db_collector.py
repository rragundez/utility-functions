import datetime as dt
import collections
import logging

import pandas as pd

from itertools import zip_longest

from db_connector import DBConnector


class DataCollector(DBConnector):
    def __init__(self, table, schema=None, **kwargs):
        self.table = self.get_full_table_name(table, schema)
        logging.info("Creating collector pointing to table '{}'"
                     .format(self.table))
        super().__init__(**kwargs)

    def collect(self, columns=None, order_by=None, ascending=True, limit=None,
                **filters):
        """Read data from the database.

        Args:
            columns (iterable[str]/None): List/tuple of columns to select.
                If None select all. Defaults to None.
            order_by (str/iterable[str]/None): Column or columns to order by.
                If None, do not order. Defaults to None.
            ascending (bool): If True, order ascending, else order descending.
                Defaults to True.
            limit (int/None): Limit the result to a maximum number of lines.
                If None, do not limit. Defaults to None.
            **filters: Filter the results. The key should correspond to a
                column in the data set. The value can be one of:
                - A number or string.
                - A datetime, date or pandas timestamp.
                - A pandas.DatetimeIndex (pandas.date_range):
                    converted to iterable of strings by .strftime('%Y-%m-%d'))
                    and do a recursion.
                - An iterable object:
                    will then do a recursion on each element.

        Returns:
            pandas.DataFrame: Data required by the arguments given.
        """

        if isinstance(columns, str):
            columns = [columns]
        if isinstance(order_by, str):
            order_by = [order_by]
        if isinstance(ascending, bool):
            ascending = [ascending]

        logging.info("Creating base query.")
        query = ("SELECT {} FROM {}"
                 .format(', '.join(c for c in columns) if columns else '*',
                         self.table))
        logging.debug(query)

        if filters:
            filter_query = self._create_sql_filter
            query = ' '.join([query, filter_query])
            logging.debug(query)
        if order_by:
            logging.info("Creating order query.")
            order_query = (
                'ORDER BY ' +
                ', '.join("{c} {o}"
                          .format(c=c, o='ASC' if a else 'DESC')
                          for c, a in zip_longest(order_by, ascending,
                                                  fillvalue=ascending[-1]))
            )
            query = ' '.join([query, order_query])
            logging.debug(query)
        if limit:
            logging.info("Creating limit query.")
            limit_query = ' '.join(["LIMIT", self.py_to_sql(limit)])
            query = ' '.join([query, limit_query])
            logging.debug(query)

        return self._query(query)

    def histogram_categories(self, column, **filters):
        logging.info("Creating histogram for column {}".format(column))

        logging.info("Creating histogram base query.")
        query = ("SELECT {}, count(*) as frequency FROM {}"
                 .format(column, self.table))
        logging.debug(query)

        if filters:
            filter_query = self._create_sql_filter
            query = ' '.join([query, filter_query])
            logging.debug(query)

        group_query = "GROUP BY {c} ORDER BY frequency DESC".format(c=column)
        query = ' '.join([query, group_query])
        logging.debug(query)

        return self._query(query)

    def _create_sql_filter(self, filters):
        logging.info("Creating filter query.")
        filter_query = (
            'WHERE ' +
            ' AND '.join("{c} IN ({v})"
                         .format(c=column, v=self.py_to_sql(values))
                         for column, values in filters.items())
        )
        return filter_query

    def histogram_numerical(self, column, **filters):
        logging.info("Creating histogram for column {}".format(column))

        logging.info("Creating histogram sub table query.")
        sub_table_query = ("SELECT CAST({c} AS int) as {c} FROM {t}"
                           .format(c=column, t=self.table))
        logging.debug(sub_table_query)

        if filters:
            filter_query = self._create_sql_filter
            sub_table_query = ' '.join([sub_table_query, filter_query])
            logging.debug(sub_table_query)

        logging.info("Creating histogram query")
        min_value_query = "(SELECT MIN({c}) FROM sub_table)".format(c=column)
        bucket_size_query = """(SELECT floor((MAX({c}) - MIN({c})) / 20)
                                FROM sub_table)""".format(c=column)
        query = """
            WITH sub_table
            AS ({st})
            SELECT
                {c}_min,
                {c}_max,
                count(*) AS frequency
            FROM (
                SELECT
                    {min_val} + floor((feature_value - {min_val})/{buclet_size})*{buclet_size} as {c}_min,
                    {min_val} + (1 + floor((feature_value - {min_val})/{buclet_size}))*{buclet_size} as {c}_max
                    FROM sub_table
            ) dummy
            GROUP BY 1, 2
            ORDER BY 1
        """.format(st=sub_table_query, c=column,
                   min_val=min_value_query, buclet_size=bucket_size_query)
        logging.debug(query)

        return self._query(query)

    @classmethod
    def py_to_sql(cls, value):
        logging.info("Converting python object {} to SQL string."
                     .format(value))
        if isinstance(value, str):
            sql = "'{}'".format(value)
        elif isinstance(value, (int, float)):
            sql = str(value)
        elif isinstance(value, dt.date):
            sql = "'{}'".format(dt.datetime.strftime(value, '%Y-%m-%d'))
        elif isinstance(value, pd.DatetimeIndex):
            sql = cls.py_to_sql(value.strftime('%Y-%m-%d'))
        elif isinstance(value, collections.Iterable):
            sql = ', '.join(cls.py_to_sql(v) for v in value)

        logging.debug("{} ---> {}".format(value, sql))

        return sql

    def get_full_table_name(table, schema):
        if schema:
            return "{}.{}".format(schema, table)
        else:
            return table
