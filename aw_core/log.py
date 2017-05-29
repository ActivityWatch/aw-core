import os
import sys
import logging
from typing import Optional, List
from datetime import datetime

from pythonjsonlogger import jsonlogger

from . import dirs

log_file_path = None


def get_log_file_path() -> Optional[str]:
    return log_file_path


def setup_logging(name: str, testing=False, verbose=False,
                  log_stderr=True, log_file=False, log_file_json=False):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if log_stderr:
        root_logger.addHandler(_create_stderr_handler())
    if log_file:
        root_logger.addHandler(_create_file_handler(name, testing=testing, log_json=log_file_json))


def _get_latest_log_files(name, testing=False) -> List[str]:
    """Returns a list with the filenames (not full paths) of all available logfiles for `name` sorted by latest first."""
    files = filter(lambda filename: name in filename, os.listdir(dirs.get_log_dir()))
    files = filter(lambda filename: "testing" in filename if testing else "testing" not in filename, files)
    return sorted(files, reverse=True)


def get_latest_log_file(name, testing=False) -> Optional[str]:
    """Returns the filename of the last logfile with `name`.
       Useful when you want to read the logfile of another ActivityWatch service."""
    last_logs = _get_latest_log_files(name, testing=testing)
    return last_logs[0] if last_logs else None


def _create_stderr_handler() -> logging.Handler:
    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.setFormatter(_create_human_formatter())

    return stderr_handler


def _create_file_handler(name, testing=False, log_json=False) -> logging.Handler:
    log_dir = dirs.get_log_dir()

    # Set logfile path and name
    global log_file_path

    # Should result in something like:
    # $LOG_DIR/aw-server_testing_2017-01-05T00:21:39.log
    file_ext = ".log.json" if log_json else ".log"
    now_str = str(datetime.now().replace(microsecond=0).isoformat()).replace(":", "-")
    log_name = name + "_" + ("testing_" if testing else "") + now_str + file_ext
    log_file_path = os.path.join(log_dir, log_name)

    fh = logging.FileHandler(log_file_path, mode='w')
    if log_json:
        fh.setFormatter(_create_json_formatter())
    else:
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
