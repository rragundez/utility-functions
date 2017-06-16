import botocore


class BucketNotFoundError(botocore.exceptions.ClientError):
    def __init__(self, message):
        self.message = message


class KeyNotFoundError(botocore.exceptions.ClientError):
    def __init__(self, message):
        self.message = message
