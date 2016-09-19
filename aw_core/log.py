import os
import sys
import logging
from typing import Optional
from datetime import datetime

import appdirs
from pythonjsonlogger import jsonlogger


log_file_path = None


def get_log_file_path() -> Optional[str]:
    return log_file_path


def setup_logging(name: str, testing=False, log_stderr=True, log_file=False, log_file_json=False):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if testing else logging.INFO)

    if log_stderr:
        root_logger.addHandler(_create_stderr_handler())
    if log_file:
        root_logger.addHandler(_create_file_handler(name, testing=testing, log_json=log_file_json))


def _create_stderr_handler() -> logging.Handler:
    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.setFormatter(_create_human_formatter())

    return stderr_handler


def _create_file_handler(name, testing=False, log_json=False) -> logging.Handler:
    # Get and create log path
    user_data_dir = appdirs.user_data_dir(name, "activitywatch")
    log_dir = os.path.join(user_data_dir, "testing" if testing else "", "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Set logfile path and name
    global log_file_path

    if log_json:
        log_file_path = os.path.join(log_dir, str(datetime.now().isoformat()) + ".log.json")
        fh = logging.FileHandler(log_file_path, mode='w')
        fh.setFormatter(_create_json_formatter())
    else:
        log_file_path = os.path.join(log_dir, str(datetime.now().isoformat()) + ".log")
        fh = logging.FileHandler(log_file_path, mode='w')
        fh.setFormatter(_create_human_formatter())

    return fh


def _create_human_formatter() -> logging.Formatter:
    return logging.Formatter('%(asctime)-15s [%(levelname)-5s]: %(message)s (%(filename)s:%(lineno)s)')


def _create_json_formatter() -> logging.Formatter:
    supported_keys = [
        'asctime',
        # 'created',
        'filename',
        'funcName',
        'levelname',
        # 'levelno',
        'lineno',
        'module',
        # 'msecs',
        'message',
        'name',
        'pathname',
        # 'process',
        # 'processName',
        # 'relativeCreated',
        # 'thread',
        # 'threadName'
    ]

    def log_format(x):
        """Used to give JsonFormatter proper parameter format"""
        return ['%({0:s})'.format(i) for i in x]

    custom_format = ' '.join(log_format(supported_keys))

    return jsonlogger.JsonFormatter(custom_format)
