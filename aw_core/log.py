import os
import sys
import logging
from typing import Optional, List
from datetime import datetime

from pythonjsonlogger import jsonlogger

from . import dirs
from .decorators import deprecated

# NOTE: Will be removed in a future version since it's not compatible with running a multi-service process
# TODO: prefix with `_`
log_file_path = None


@deprecated
def get_log_file_path() -> Optional[str]:  # pragma: no cover
    """DEPRECATED: Use get_latest_log_file instead."""
    return log_file_path


def setup_logging(
    name: str,
    testing=False,
    verbose=False,
    log_stderr=True,
    log_file=False,
    log_file_json=False,
):  # pragma: no cover
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    root_logger.handlers = []

    if log_stderr:
        root_logger.addHandler(_create_stderr_handler())
    if log_file:
        root_logger.addHandler(
            _create_file_handler(name, testing=testing, log_json=log_file_json)
        )

    def excepthook(type_, value, traceback):
        root_logger.exception("Unhandled exception", exc_info=(type_, value, traceback))
        # call the default excepthook if log_stderr isn't true (otherwise it'll just get duplicated)
        if not log_stderr:
            sys.__excepthook__(type_, value, traceback)

    sys.excepthook = excepthook


def _get_latest_log_files(name, testing=False) -> List[str]:  # pragma: no cover
    """Returns a list with the paths of all available logfiles for `name` sorted by latest first."""
    log_dir = dirs.get_log_dir(name)
    files = filter(lambda filename: name in filename, os.listdir(log_dir))
    files = filter(
        lambda filename: "testing" in filename
        if testing
        else "testing" not in filename,
        files,
    )
    return [os.path.join(log_dir, filename) for filename in sorted(files, reverse=True)]


def get_latest_log_file(name, testing=False) -> Optional[str]:  # pragma: no cover
    """
    Returns the filename of the last logfile with ``name``.
    Useful when you want to read the logfile of another ActivityWatch service.
    """
    last_logs = _get_latest_log_files(name, testing=testing)
    return last_logs[0] if last_logs else None


def _create_stderr_handler() -> logging.Handler:  # pragma: no cover
    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.setFormatter(_create_human_formatter())

    return stderr_handler


def _create_file_handler(
    name, testing=False, log_json=False
) -> logging.Handler:  # pragma: no cover
    log_dir = dirs.get_log_dir(name)

    # Set logfile path and name
    global log_file_path

    # Should result in something like:
    # $LOG_DIR/aw-server_testing_2017-01-05T00:21:39.log
    file_ext = ".log.json" if log_json else ".log"
    now_str = str(datetime.now().replace(microsecond=0).isoformat()).replace(":", "-")
    log_name = name + "_" + ("testing_" if testing else "") + now_str + file_ext
    log_file_path = os.path.join(log_dir, log_name)

    fh = logging.FileHandler(log_file_path, mode="w")
    if log_json:
        fh.setFormatter(_create_json_formatter())
    else:
        fh.setFormatter(_create_human_formatter())

    return fh


def _create_human_formatter() -> logging.Formatter:  # pragma: no cover
    return logging.Formatter(
        "%(asctime)s [%(levelname)-5s]: %(message)s  (%(name)s:%(lineno)s)",
        "%Y-%m-%d %H:%M:%S",
    )


def _create_json_formatter() -> logging.Formatter:  # pragma: no cover
    supported_keys = [
        "asctime",
        # 'created',
        "filename",
        "funcName",
        "levelname",
        # 'levelno',
        "lineno",
        "module",
        # 'msecs',
        "message",
        "name",
        "pathname",
        # 'process',
        # 'processName',
        # 'relativeCreated',
        # 'thread',
        # 'threadName'
    ]

    def log_format(x):
        """Used to give JsonFormatter proper parameter format"""
        return ["%({0:s})".format(i) for i in x]

    custom_format = " ".join(log_format(supported_keys))

    return jsonlogger.JsonFormatter(custom_format)
