from deduplicator import constants as c
from deduplicator.logger import logger
from pathlib import Path
import boto3
import yaml

configuration_file_path = Path(c.CONFIGURATION_PATH)
configuration_file_path.touch(mode=0o666)
with configuration_file_path.open(mode='r') as stream:
    try:
        configuration = (yaml.safe_load(stream))
    except yaml.YAMLError as exc:
        logger.info(exc)

s3_profile_name = configuration['S3']['profile']

logger.info(f"Using AWS profile: {s3_profile_name}")

session = boto3.session.Session(profile_name=s3_profile_name)
s3 = session.resource('s3')
for bucket in s3.buckets.all():
    logger.info(bucket)
