from deduplicator import constants as c
from logging.handlers import WatchedFileHandler
from pythonjsonlogger import jsonlogger
import datetime
import logging
from pathlib import Path


class JsonLogFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(JsonLogFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        log_record['level'] = record.levelname


logger = logging.getLogger('deduplicator')
if not logger.handlers:
    # json_formatter = JsonLogFormatter('(timestamp) (level) (funcName) (filename) (lineno) (message)')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)

    log_file_path = Path(c.LOG_FILE_PATH)
    log_file_path.touch(mode=0o666)

    # file_handler = WatchedFileHandler(c.LOG_FILE_PATH)
    # file_handler.setFormatter(json_formatter)
    # logger.addHandler(file_handler)

    streaming_handler = logging.StreamHandler()
    streaming_handler.setFormatter(console_formatter)
    logger.addHandler(streaming_handler)


