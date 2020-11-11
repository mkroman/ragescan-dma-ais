#!/usr/bin/env python

import os
import sys
import logging
import subprocess

from ragescan import dma, ezs3

# List of files or folders that are ignored on the ftp server
IGNORED_FILES = ['aspnet_client']

# The default temporary work directory
TEMPORARY_WORK_DIR = 'tmp'

def process_original_file_name(file_name):
  """Parses the given `file_name` and returns a tuple of (basename, archive_type, extension)"""
  base_name, extension = os.path.splitext(file_name)
  archive_type = extension[1:] if extension in ['.rar', '.zip'] else None

  return base_name, archive_type, extension

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ragescan')

# Create the temporary work dir if it doesn't exist
if not os.path.exists(TEMPORARY_WORK_DIR):
  os.mkdir(TEMPORARY_WORK_DIR)

# Get a full list of all the file names in the bucket
s3 = ezs3.S3('danish-ais-data')
bucket_objects = s3.ls()
bucket_file_names = list(map(lambda x: x.key, bucket_objects))

# Connect to the FTP server
ftp = dma.FTP()
ftp.cd('ais_data')

ftp_files = ftp.ls()



# Go through each of the files on the FTP server and process it if it's not
# present in the s3 bucket
for file_name in ftp_files:
  if file_name in IGNORED_FILES:
    continue

  base_name, archive_type, extension = process_original_file_name(file_name)
  processed_file_name = f"{base_name}.tar.zst" if archive_type else f"{base_name}{extension}.zst"

  if not processed_file_name in bucket_file_names:
    local_file_path = os.path.join(TEMPORARY_WORK_DIR, file_name)
    local_processed_file_path = os.path.join(TEMPORARY_WORK_DIR, processed_file_name)

    if not os.path.exists(local_file_path):
      logger.info(f'Downloading {file_name} to {local_file_path}')

      with open(local_file_path, 'wb') as f:
        ftp.download(file_name, f.write)

    if archive_type == 'zip':
      logger.info(f'Recompressing {file_name} with zip2zstd')
      command = ["zip2zstd", local_file_path, local_processed_file_path]
      logger.debug(command)
      result = subprocess.run(command)

      if result.returncode != 0:
        logger.error(f'Failed to recompress {file_name}')
        continue
      else:
        # Delete the original file
        logger.debug(f'Deleting {local_file_path}')
        os.unlink(local_file_path)
    else:
      logger.info(f'Compressing {file_name} with zstd')
      command = ["zstd", "-f", "-T5", "-11", local_file_path]
      logger.debug(command)
      result = subprocess.run(command)

      if result.returncode != 0:
        logger.error(f'Failed to compress {file_name}')
        continue
      else:
        # Delete the original file
        logger.debug(f'Deleting {local_file_path}')
        os.unlink(local_file_path)

    # Upload the processed file
    s3.upload(local_processed_file_path, processed_file_name, storage_class='GLACIER')

