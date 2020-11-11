import os
import logging

import boto3
import botocore
from boto3.s3.transfer import TransferConfig

class S3:
  """EZS3 is an easy to use wrapper around an S3 bucket"""

  def __init__(self, bucket_name, **kwargs):
    """Constructs a new S3 wrapper with the given settings

    Arguments:
    bucket_name -- The bucket name to use

    Keyword arguments:
    region_name -- The S3 region name (defaults to environment variable AWS_REGION_NAME)
    access_key_id -- The AWS access key ID (defaults to environment variable AWS_ACCESS_KEY_ID)
    secret_access_key -- The AWS secret access key (defaults to environment variable AWS_SECRET_ACCESS_KEY)
    endpoint_url -- The endpoint URL (defaults to environment variable AWS_ENDPOINT_URL)
    multipart_chunksize -- The size of each chunk in a multipart upload (default 200 MiB)
    """
    self.logger = logging.getLogger(__name__)
    self.session = boto3.session.Session()
    self.transfer_config = TransferConfig(multipart_chunksize=kwargs.get('multipart_chunksize',
      200 * 1024 * 1024))
    self.s3 = self.session.resource('s3',
        region_name=kwargs.get('region_name', os.environ['AWS_REGION_NAME']),
        aws_access_key_id=kwargs.get('access_key_id', os.environ['AWS_ACCESS_KEY_ID']),
        aws_secret_access_key=kwargs.get('secret_access_key', os.environ['AWS_SECRET_ACCESS_KEY']),
        endpoint_url=kwargs.get('endpoint_url', os.environ['AWS_ENDPOINT_URL']))
    self.bucket = self.s3.Bucket(bucket_name)
    self.bucket_name = bucket_name
    self.client = self.s3.meta.client

  def ls(self, prefix=""):
    """Returns a list of object summaries for objects at the given prefix"""
    return self.bucket.objects.filter(Prefix=prefix)

  def upload(self, path, key, storage_class='STANDARD'):
    """Uploads the file at the given `path` to the bucket, using the given `key`"""
    self.logger.debug(f'Uploading file {path} with key {key} using storage class {storage_class}')
    self.bucket.upload_file(path, key, ExtraArgs={'StorageClass': storage_class}, Config=self.transfer_config)
