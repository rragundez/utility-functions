import io
import os
import logging
import re
import requests
import shutil
import subprocess

from pathlib import PurePath

import botocore
import boto3
import hdfs3
import pandas as pd

from ftplib import FTP
from zipfile import ZipFile


def explode_str_column(df, column, split_on=';'):
    """Split strings in column and make row for each resulting element.
    """
    exploded = pd.DataFrame(df[column].str.split(split_on, expand=True)
                                      .stack(dropna=True)
                                      .str.strip()
                                      .reset_index(level=1, drop=True),
                            columns=[column])
    return df.drop(column, axis=1).join(exploded)
