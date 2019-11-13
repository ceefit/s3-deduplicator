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

s3_profile_name = configuration['S3']['profile']
logger.info(f"Using AWS profile: {s3_profile_name}")
session = boto3.session.Session(profile_name=s3_profile_name)
s3 = session.resource('s3')

table_name = 's3_objects'
cursor = None

try:
    connection = apsw.Connection("s3_objects.db")
    cursor = connection.cursor()
except Exception as ex:
    logger.fatal(ex)
    exit(1)

if cursor is None:
    logger.fatal("No cursor created")
    exit(1)


def get_object_count():
    total_file_count = 0

    for bucket in s3.buckets.all():
        bucket_file_count = sum(1 for _ in bucket.objects.all())
        print(f"{bucket.name}: {bucket_file_count}")
        total_file_count += bucket_file_count

    print(f"Total file count: {total_file_count}")


def write_to_db():
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (bucket STRING, key STRING, e_tag STRING, size INT)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_bucket ON '{table_name}' (bucket);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_key ON '{table_name}' (key);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_size ON '{table_name}' (size);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_size ON '{table_name}' (etag);")

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


def delete_query():
    files_to_delete = []
    for file in cursor.execute("SELECT bucket, key FROM s3_objects WHERE key = '%yourthinghere%';"):
        files_to_delete.append((file[0], file[1]))
    iteration_count = 0
    commits_made = 0
    objects_to_delete_from_db = []
    for file in files_to_delete:
        s3.Object(file[0], file[1]).delete()
        objects_to_delete_from_db.append((file[0], file[1]))
        if iteration_count % 100 == 0:
            commits_made += 1
            iteration_count = 0
            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany("DELETE from s3_objects WHERE bucket=? AND key=?;", objects_to_delete_from_db)
            cursor.execute("COMMIT;")
            objects_to_delete_from_db = []
            print(f"==============={commits_made}===================")
        iteration_count += 1
        print(file)
    cursor.execute("BEGIN TRANSACTION;")
    cursor.executemany("DELETE from s3_objects WHERE bucket=? AND key=?;", objects_to_delete_from_db)
    cursor.execute("COMMIT;")


def delete_files(files_to_delete):
    files_to_delete = [

    ]
    for file in files_to_delete:
        print((file[0], file[1]))
        cursor.execute(f"DELETE from {table_name} WHERE bucket == '{file[0]}' AND  key == '{file[1]}';")
        print(s3.Object(file[0], file[1]).delete())


# delete_files(files_to_delete)
delete_query()
# write_to_db()
