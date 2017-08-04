def create_spark_session(name='not provided', **kwargs):
    r"""
    Returns a Spark session.

    :param database str: The database name. Only used to name the SparkSession
    :param table str: The table name. Only used to name the SparkSession

    :Keyword Arguments:

    * *key* (``str``) --
      All arguments valid for SparkSession.builder, such as `master=local`
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        import findspark
        findspark.init(spark_home=kwargs.get('spark_home'),
                       python_path=kwargs.get('python_path'))
        from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in kwargs.items():
        builder = builder.config(key, value)
    spark = (builder.enableHiveSupport()
                    .appName(name)
                    .getOrCreate())
    return spark


def execute_sql(sql_query, **kwargs):
    spark = kwargs.get('spark') or create_spark_session(**kwargs)
    for query in sql_query.split(';'):
        query = query.strip()
        if query:
            logging.info("Spark executing sql query {}".format(query))
            spark.sql(query)
