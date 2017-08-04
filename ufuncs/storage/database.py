import logging
import pandas as pd
import sqlalchemy as sql


def make_db_engine(host, database, user, password, port=5432):
    """Make sqlalchemy engine.

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
    url = ("postgresql://{user}:{passw}@{host}:{port}/{db}"
           .format(user=user,
                   passw=password,
                   host=host,
                   port=port,
                   db=database))
    engine = sql.create_engine(url)
    logging.info("Postgresql engine created with user {}, host {}, "
                 "port {} and database {}"
                 .format(user, host, port, database))
    return engine


def table_exists(table, schema, engine):
    """Check if table exists inside a schema.

    Args:
        table (str): Table name.
        schema (str): The schema name.
        engine (sqlalchemy.engine.base.Engine): Engine containing the
            credentials to use to connect to the database.

    Returns:
        bool: Does the table exists or not.
    """
    query = ("select tablename from pg_tables where schemaname='{}';"
             .format(schema))
    return table in pd.read_sql(query, engine).tablename.tolist()


def df_to_table(df, table, schema, engine, update=False):
    """Write oandas dataframe to a table.

    It is important to note that the index of the dataframe is taken as
    the being the primary key in the table. At the moment works only
    with one primary key.

    This function is a sort of wrapper to the pandas dataframe to_sql
    method. This function adds some functionality. It can update
    records which have the same primary key or to only write does
    records in the dataframe which primary key is not in the database.

    If the table does not exist it creates it and makes the index of the
    dataframe a primary key.

    Args:
        df (pandas.Dataframe): The dataframe to write.
        table (str): The table name.
        schema (str): The schema name.
        engine (sqlalchemy.engine.base.Engine): Engine containing the
            credentials to use to connect to the database.
        update (bool): If True records with the same primary key will
            updated. If False it will only write the records with a
            non-existing primary key in the table. Defaults to False.
    """
    primary_key = df.index.name
    params = {'key': primary_key,
              'table': table,
              'cases': df.index.str.cat(sep="', '")}
    if update:
        # delete records in table whit same primary key
        df_drop_cases = []
        query = ("DELETE FROM {table} WHERE {key} IN ('{cases}');"
                 .format(**params))
        logging.debug(query)
        with engine.connect() as conn:
            conn.execute(query)
    else:
        # delete records in dataframe with same primary key
        query = ("SELECT {key} FROM {table} WHERE {key} IN ('{cases}');"
                 .format(**params))
        logging.debug(query)
        df_drop_cases = pd.read_sql(query, engine)[primary_key]

    if table_exists(table, schema, engine):
        (df.drop(df_drop_cases)
           .to_sql(table, engine, schema=schema, if_exists='append'))
    else:
        logging.info("Table {} doesn't exist and it will be created"
                     .format(table))
        (df.drop(df_drop_cases)
           .to_sql(table, engine, schema=schema, if_exists='append'))
        with engine.connect() as con:
            query = ("ALTER TABLE {table} ADD PRIMARY KEY ({key});"
                     .format(**params))
            logging.info(query)
            con.execute(query)
