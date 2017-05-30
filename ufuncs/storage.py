import os
import pickle
import logging

from pathlib import PurePath

import botocore
import boto3
import hdfs3


def to_local(obj, *, fullpath, permission_code=755, overwrite=True):
    if overwrite or not os.path.isfile(fullpath):
        os.makedirs(os.path.dirname(fullpath), exist_ok=True)
        with open(fullpath, 'wb') as f:
            pickle.dump(obj, f)
        os.chmod(fullpath, int(str(permission_code), 8))
    else:
        raise IOError("Local file '{}' already exists. Argument overwrite "
                      "is {}".format(fullpath, overwrite))


def from_local(fullpath):
    with open(fullpath, 'rb') as f:
        obj = pickle.load(f)
        return obj


def to_hdfs(obj, fullpath, permission_code=755, overwrite=True):
    hdfs = hdfs3.HDFileSystem()
    if overwrite or not hdfs.exists(fullpath):
        hdfs.mkdir(os.path.dirname(fullpath))
        with hdfs.open(fullpath, 'wb') as f:
            f.write(pickle.dumps(obj, -1))
        hdfs.chmod(fullpath, int(str(permission_code), 8))
    else:
        raise IOError("HDFS file '{}' already exists. Argument overwrite "
                      "is {}".format(fullpath, overwrite))


def from_hdfs(fullpath):
    hdfs = hdfs3.HDFileSystem()
    with hdfs.open(fullpath, 'rb') as f:
        obj = pickle.loads(f.read())
    return obj


def to_s3(obj, fullpath, *, overwrite=True, **kwargs):
    s3 = boto3.resource('s3', region_name=kwargs.pop('region_name', None))
    split_path = PurePath(fullpath).parts
    bucket_name = split_path[1]
    key = os.path.join(*split_path[2:])

    bucket = s3.Bucket(bucket_name)
    versioning = bucket.Versioning().status
    if overwrite or not s3_exists(fullpath) or versioning == 'Enabled':
        return bucket.Object(key).put(Body=pickle.dumps(obj, -1), **kwargs)
    else:
        raise IOError("S3 object '{}' already exists and Bucket versioning "
                      "is {}. Agument overwrite is {}"
                      .format(fullpath, versioning, overwrite))


def from_s3(fullpath):
    s3 = boto3.resource('s3')
    split_path = PurePath(fullpath).parts
    bucket_name = split_path[1]
    key = os.path.join(*split_path[2:])

    return pickle.loads(s3.Object(bucket_name, key).get()['Body'].read())


def s3_exists(fullpath, raise_error=False):
    s3 = boto3.resource('s3')
    split_path = PurePath(fullpath).parts
    bucket_name = split_path[1]

    bucket = s3.Bucket(bucket_name)
    try:
        bucket.load()
    except botocore.exceptions.ClientError as e:
        exists = False
    else:
        exists = True

    if len(split_path) > 2 and exists:
        try:
            bucket.Object(os.path.join(*split_path[2:])).load()
        except botocore.exceptions.ClientError as e:
            exists = False
        else:
            exists = True
    return exists


def hdfs_chmod_dirs(path, permission, hdfs3_obj=None):
    """Change permissions of each direcctory in the fullpath

    Args:
        path (str): Path in HDFS
        permission (int/str): Permission code (eg. 777, 775)
        hdfs3_obj (hdfs3.HDFileSystem): HDFS connector
    """
    hdfs = hdfs3_obj if hdfs3_obj else hdfs3.HDFileSystem()
    _path = '/'
    for d in path.split('/')[1:]:
        _path = os.path.join(_path, d)
        try:
            hdfs.chmod(_path, int(str(permission), 8))
        except IOError as e:
            logging.warning("Cannot change permissions of {}: "
                            "{}".format(_path, str(e)))
