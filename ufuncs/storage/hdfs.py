import os
import pickle

from pathlib import PurePath

import hdfs3
import pandas as pd

from ufuncs.storage.utils import check_abs_path


def hdfs_chmod_dirs(path, *, permission_code, raise_errors=False, hdfs3_conn=None):
    """Try to change permissions of each part of the path.

    Args:
        path (str): Path in HDFS
        permission_code (int/str): Permission to set on each part of the
            path (eg. 777, 775).
        hdfs3_conn (hdfs3.core.HDFileSystem): HDFS connector.

    Raises:
        IOError: If not possible to change permission of a path part.
    """
    check_abs_path(path)
    hdfs = hdfs3_conn if hdfs3_conn else hdfs3.HDFileSystem()
    # change permissions starting with top dir
    _path = '/'
    for d in PurePath(path).parts[1:]:
        _path = os.path.join(_path, d)
        try:
            hdfs.chmod(_path, int(str(permission_code), 8))
        except IOError as e:
            if raise_errors:
                raise e


def hdfs_put_object(obj, storage_path,
                    *, permission_code=755, overwrite=True, hdfs_conn=None):
    """Store a python object to HDFS in pickle file.

    Args:
        obj: Python object.
        storage_path (str): HDFS full path of the file to write to.
        permission_code (int/str): Permission to set on the pickle file
            (eg. 777, 775). Defaults to 755.
        overwrite (bool): Overwrite if file already exists.
            Defaults to True.
        hdfs_conn (hdfs3.core.HDFileSystem): HDFS connector.

    Raises:
        FileExistsError: If file exists and overwrite is set to False.
    """
    # create connector if not exists
    hdfs = hdfs_conn if hdfs_conn else hdfs3.HDFileSystem()
    if not overwrite and hdfs.exists(storage_path):
        raise FileExistsError("HDFS file '{}' already exists. "
                              "Argument overwrite is {}."
                              .format(storage_path, overwrite))
    hdfs.mkdir(os.path.dirname(storage_path))
    with hdfs.open(storage_path, 'wb') as f:
        pickle.dump(obj, f)
    hdfs.chmod(storage_path, int(str(permission_code), 8))


def hdfs_get_object(storage_path, *, hdfs_conn=None):
    """Retrieve a python object from a pickle file in HDFS.

    Args:
        storage_path (str): HDFS full path of the file to write to.
        hdfs_conn (hdfs3.core.HDFileSystem): HDFS connector.

    Returns:
        The python object that was loaded from the pickle file.

    Raises:
        NameError: If the object's class is not defined in the namespace.
        FileNotFoundError: If file is not found.
    """
    hdfs = hdfs_conn if hdfs_conn else hdfs3.HDFileSystem()
    try:
        with hdfs.open(storage_path, 'rb') as f:
            obj = pickle.load(f)
    except IOError as _:
        raise FileNotFoundError("No such file or directory in HDFS: '{}'."
                                .format(storage_path))
    except AttributeError as _:
        raise NameError("Pickle file object class not found in the namespace.")
    return obj


def hdfs_df2csv(df, storage_path, *,
                permission_code=755, overwrite=True, hdfs_conn=None, **kwargs):
    """Save pandas dataframe to csv in HDFS.

    The kwargs are used to represent any argument from the known
    pandas.DataFrame.to_csv function.

    Args:
        df (pandas.DataFrame): Dataframe to write as csv
        permission_code (int/str): Permission to set on the pickle file
            (eg. 777, 775). Defaults to 755.
        overwrite (bool): Overwrite if file already exists.
            Defaults to True.
        hdfs_conn (hdfs3.core.HDFileSystem): HDFS connector.

    Raises:
        TypeError: If df is not a pandas DataFrame
        FileExistsError: If file exists and overwrite is set to False.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected pandas Dataframe, got {}"
                        .format(type(df).__name__))
    check_abs_path(storage_path)
    # make hdfs connection
    hdfs = hdfs_conn if hdfs_conn else hdfs3.HDFileSystem()
    # check if file to overrite
    if not overwrite and hdfs.exists(storage_path):
        raise FileExistsError("HDFS file '{}' already exists. "
                              "Argument overwrite is {}."
                              .format(storage_path, overwrite))
    # make directories
    hdfs.mkdir(os.path.dirname(storage_path))
    # write csv bytes to HDFS file
    with hdfs.open(storage_path, 'wb') as f:
        f.write(df.to_csv(**kwargs))
    # change permission
    hdfs.chmod(storage_path, int(str(permission_code), 8))


def hdfs_csv2df(storage_path, *, hdfs_conn=None, **kwargs):
    """Read .csv from HDFS into a pandas dataframe.

    The kwargs are used to represent any argument from the known
    pandas.DataFrame.read_csv function.

    Args:
        storage_path (str): Location of .csv file in HDFS
        hdfs_conn (hdfs3.core.HDFileSystem): HDFS connector.

    Returns:
        pd.DataFrame: Dataframe with .csv data
    """
    check_abs_path(storage_path)
    hdfs = hdfs_conn if hdfs_conn else hdfs3.HDFileSystem()
    with hdfs.open(storage_path, 'rb') as f:
        df = pd.read_csv(f, **kwargs)
    return df
