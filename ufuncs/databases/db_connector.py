import logging
import os

import pandas as pd
import pyodbc

from pyodbc import ProgrammingError
from sqlalchemy import create_engine


class DBConnector(object):
    """Class to stablish a connection to a database and execute queries.

    The class holds no credentials as attributes since they are private.
    If the credentials are not given at instantiation it will look for
    the respective environment variables.
    """

    def __init__(self, **kwargs):
        self.conn = self.connect(**kwargs)

    def connect(self, **kwargs):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def _query(self, sql):
        """Execute a given query.

        Only SELECT statements are allowed.

        Args:
            sql (str): SQL query string.

        Returns:
            pandas.DataFrame: Resulting dataframe from the SQL query.
        """
        sql = sql.strip()
        logging.info("Executing query on database '{}': '{}'"
                     .format(self.database, sql))

        if not sql.lower().startswith('select'):
            raise AssertionError("Only 'SELECT' queries are allowed.")

        data = pd.read_sql_query(sql, self.conn)
        logging.info("Retrieved data: rows={}  columns={}".format(*data.shape))

        if data.empty:
            logging.warning("No data found with query: {}". format(sql))
        return data


class ODBCConnector(DBConnector):

    def __init__(self, **kwargs):
        # pre check on kwargs for example can be used here
        super.__init__(**kwargs)

    def connect(self, **kwargs):
        """Make a connection with the database.
        """
        try:
            conn_details = {
                'driver': kwargs.get('driver', os.environ['DRIVER']),
                'server': kwargs.get('server', os.environ['SERVER']),
                'port': kwargs.get('port', os.environ['PORT']),
                'database': kwargs.get('database', os.environ['DATABASE']),
                'username': kwargs.get('username', os.environ['USER']),
                'password': kwargs.get('password', os.environ['PASSWORD'])
            }
        except KeyError as e:
            msg = "Environment variable not found: {}".format(str(e))
            logging.error(msg)
            raise KeyError(msg)
        conn_str = ("DRIVER={driver};"
                    "SERVER={server};"
                    "PORT={port};"
                    "DATABASE={database};"
                    "UID={username};"
                    "PWD={password}"
                    .format(**conn_details))
        conn = pyodbc.connect(conn_str)
        # extra optional changes
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')
        conn.setencoding(encoding='utf-8')
        return conn

    def close(self):
        try:
            logging.info("Closing connection")
            self.conn.close()
        except ProgrammingError as e:
            logging.warning(str(e))


class PostGreSQLConnector(DBConnector):
    """Make sqlalchemy connection
    http://docs.sqlalchemy.org/en/latest/core/engines.html

    Args:
        host (str): The name of the host
        database (str): The database name
        user (str): The username
        password (str): Database password
        port (int|str): The port number
    Returns:
        sqlalchemy.engine.base.Engine: sqlalchemy object from which we
            can create a pool a connections or execute queries directly.
    """

    def __init__(self, **kwargs):
        # pre check on kwargs for example can be used here
        super.__init__(**kwargs)

    def connect(self, **kwargs):
        """Make a connection with the database.
        """
        try:
            conn_details = {
                'host': kwargs.get('host', os.environ['HOST']),
                'port': kwargs.get('port', os.environ['PORT']),
                'database': kwargs.get('database', os.environ['DATABASE']),
                'username': kwargs.get('username', os.environ['USER']),
                'password': kwargs.get('password', os.environ['PASSWORD'])
            }
        except KeyError as e:
            msg = "Environment variable not found: {}".format(str(e))
            logging.error(msg)
            raise KeyError(msg)
        conn_str = ("postgresql://{user}:{password}@{host}:{port}/{database}"
                    .format(**conn_details))
        conn = create_engine(conn_str).connect()
        return conn

    def close(self):
        self.conn.close()
