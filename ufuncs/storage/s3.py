import os
import pickle

from pathlib import PurePath

import botocore
import boto3

from ufuncs.storage.helpers import BucketNotFoundError
from ufuncs.storage.helpers import KeyNotFoundError
from ufuncs.storage.utils import check_abs_path


def s3_split_path(path):
    """Get bucket and key from path.

    If only the path does not contain a key, key will be returned as None.

    Args:
        path (str): s3 full path. This path is compose by '/<bucket>/<key>'.

    Returns:
        tuple(str, str): The name of the bucket and the key of the object.

    Raises:
        TypeError: If path given is not absolute.
    """
    check_abs_path(path)
    split_path = PurePath(path).parts
    bucket_name = split_path[1]
    key_parts = split_path[2:]
    if key_parts:
        key = os.path.join(*key_parts)
    else:
        key = None
    return bucket_name, key


def s3_exists(path, *, raise_error=False, s3_conn=None, **kwargs):
    """Check if bucket and key from path exist.

    If `raise_error` is True the the function will raise an exception
    if the bucket or the key is not found.

    Args:
        path (str): s3 full path. This path is compose by '/<bucket>/<key>'.
        raise_error (bool): Raise error if bucket or key not found.
        s3_conn (boto3.resources.factory.s3.ServiceResource): s3 connector.

    Retuns:
        bool: True if bucket and key are found, False otherwise.

    Raises:
        TypeError: If path given is not absolute.
        BucketNotFoundError: If bucket not found.
        KeyNotFoundError: If key not found in bucket.
    """
    check_abs_path(path)
    # create s3 connection
    s3 = s3_conn if s3_conn else boto3.resource('s3', **kwargs)
    # get bucket and key name
    bucket_name, key = s3_split_path(path)
    bucket = s3.Bucket(bucket_name)
    # check bucket exists
    try:
        bucket.load()
    except botocore.exceptions.ClientError as e:
        if raise_error:
            raise BucketNotFoundError("No such bucket '{}' in s3"
                                      .format(bucket_name))
        exists = False
    else:
        exists = True
    # check key exists
    if key and exists:
        try:
            bucket.Object(key).load()
        except botocore.exceptions.ClientError as e:
            if raise_error:
                raise KeyNotFoundError("No such key '{}' in bucket '{}' "
                                       "in s3".format(key, bucket_name))
            exists = False
        else:
            exists = True
    return exists


def s3_put_object(obj, storage_path, *, overwrite=True, s3_conn=None, **kwargs):
    """Store a python object to s3 in pickle file.

    Args:
        obj: Python object.
        storage_path (str): s3 full path of the file to write to.
            This path is compose by '/<bucket>/<key>'.
        overwrite (bool): Overwrite if file already exists.
            Defaults to True
        s3_conn (boto3.resources.factory.s3.ServiceResource): s3 connector.

    Raises:
        TypeError: If storage_path given is not absolute.
        FileExistsError: If file exists, overwrite is set to False and
            versioning is not active.
    """
    # create s3 connection
    check_abs_path(storage_path)
    if not s3_conn:
        s3 = boto3.resource('s3', region_name=kwargs.pop('region_name', None))

    bucket_name, key = s3_split_path(storage_path)
    bucket = s3.Bucket(bucket_name)
    # check if versioning is enabled in the bucket
    versioning = bucket.Versioning().status
    # write object
    if overwrite or not s3_exists(storage_path, s3_conn=s3) or versioning == 'Enabled':
        return bucket.Object(key).put(Body=pickle.dumps(obj, -1), **kwargs)
    else:
        raise FileExistsError("S3 object '{}' already exists and versioning "
                              "is {}. Argument overwrite is set to {}."
                              .format(storage_path, versioning, overwrite))


def s3_get_object(storage_path, *, s3_conn=None, **kwargs):
    """Retrieve a python object from a pickle file in s3.

    Args:
        storage_path (str): s3 full path of the file to write to.
            This path is compose by '/<bucket>/<key>'.
        s3_conn (boto3.resources.factory.s3.ServiceResource): s3 connector.

    Returns:
        The python object that was loaded from the pickle file.

    Raises:
        TypeError: If storage_path given is not absolute.
        NameError: If the object's class is not defined in the namespace.
        BucketNotFoundError: If bucket not found.
        KeyNotFoundError: If key not found in bucket.
    """
    check_abs_path(storage_path)
    s3 = s3_conn if s3_conn else boto3.resource('s3',
                                                region_name=kwargs.pop('region_name', None))
    # raise exceptions if bucket or key don't exist
    s3_exists(storage_path, raise_error=True, s3_conn=s3)
    bucket_name, key = s3_split_path(storage_path)
    try:
        obj = pickle.loads(s3.Object(bucket_name, key).get()['Body'].read())
    except AttributeError as _:
        raise NameError("Pickle file object class not found in the namespace.")
    return obj
