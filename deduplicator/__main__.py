from csv import excel

from deduplicator import constants as c
from deduplicator.logger import logger
from pathlib import Path
import apsw
import boto3
import yaml

configuration_file_path = Path(c.CONFIGURATION_PATH)
configuration_file_path.touch(mode=0o666)
with configuration_file_path.open(mode='r') as stream:
    try:
        configuration = (yaml.safe_load(stream))
    except yaml.YAMLError as exc:
        logger.info(exc)

table_name = 's3_objects'

cursor = None
database_is_initialized = False

try:
    connection = apsw.Connection("s3_objects.db")
    cursor = connection.cursor()
    for row in cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"):
        database_is_initialized = True
except Exception as ex:
    connection.close(True)
    logger.fatal(ex)
    exit(1)

if cursor is None:
    logger.fatal("No cursor created")
    exit(1)

cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (bucket STRING, key STRING, e_tag STRING, size INT)")
cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_bucket ON '{table_name}' (bucket);")
cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_key ON '{table_name}' (key);")
cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_size ON '{table_name}' (size);")

s3_profile_name = configuration['S3']['profile']

logger.info(f"Using AWS profile: {s3_profile_name}")

session = boto3.session.Session(profile_name=s3_profile_name)
s3 = session.resource('s3')

for bucket in s3.buckets.all():
    bucket_file_count = 0
    iteration_count = 0

    logger.info(f"Grabbing {bucket.name}")
    batch_insert_values = list()
    cursor.execute("BEGIN TRANSACTION;")

    for s3_object in bucket.objects.all():
        # Trim the quotes off the e_tag, so remove the 1st and last characters
        batch_insert_values.append((s3_object.bucket_name, s3_object.key, s3_object.e_tag[1:-1], s3_object.size))
        iteration_count += 1
        bucket_file_count += 1
        if iteration_count % 10000 == 0:
            iteration_count = 0
            cursor.executemany(f"INSERT INTO {table_name} VALUES(?, ?, ?, ?)", batch_insert_values)
            cursor.execute("COMMIT;")
            batch_insert_values = list()
            cursor.execute("BEGIN TRANSACTION;")
            logger.info(f"Committed {bucket_file_count} objects from {bucket.name}")
    cursor.executemany(f"INSERT INTO {table_name} VALUES(?, ?, ?, ?)", batch_insert_values)
    cursor.execute("COMMIT;")
logger.info("Complete")
