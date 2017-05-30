import io
import os
import hdfs3
import pandas as pd


def hdfs_df2csv(df, path,
                hdfs3_obj=None, permission=755, overwrite=True, **kwargs):
    """Save pandas dataframe to csv in HDFS

    The kwargs are used to represent any argument from the known
    pandas.to_csv function.

    Args:
        df (pd.DataFrame): Dataframe to write as csv
        hdfs3_obj (hdfs3.HDFileSystem): HDFS connector
        permission (int/str): Permission code to apply to file.
        overwrite (bool): If True will overwrite an existing file, If
            False and file already exists will raise an IOError.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected pandas Dataframe, got {}"
                        .format(type(df).__name__))

    hdfs = hdfs3_obj if hdfs3_obj else hdfs3.HDFileSystem()
    if not overwrite and hdfs.exists(path):
        raise IOError("HDFS file '{}' already exists. Argument overwrite "
                      "is {}".format(path, overwrite))
    # make directories
    hdfs.mkdir(os.path.dirname(path))
    # make temporary csv and move to HDFS
    tmp_file = '/tmp/tmp.csv'
    df.to_csv(tmp_file, encoding='utf-8', **kwargs)
    hdfs.put(tmp_file, path)
    os.remove(tmp_file)
    # change permission
    hdfs.chmod(path, int(str(permission), 8))


def hdfs_csv2df(path, hdfs3_obj=None, **kwargs):
    """Read .csv from HDFS into a pandas dataframe

    The kwargs are used to represent any argument from the known
    pandas.read_csv function.

    Args:
        path (str): Location of .csv file in HDFS
        hdfs3_obj (hdfs3.HDFIleSystem): Optional HDFS connector

    Returns:
        pd.DataFrame: Dataframe with .csv data
    """
    hdfs = hdfs3_obj if hdfs3_obj else hdfs3.HDFileSystem()
    return pd.read_csv(io.StringIO(hdfs.cat(path).decode('utf-8')))
