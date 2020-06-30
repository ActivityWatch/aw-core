import warnings
import functools
import logging
import time


def deprecated(f):  # pragma: no cover
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used.

    Taken from: http://stackoverflow.com/a/30253848/965332
    """
    # Warn only once per deprecated function
    warned_for = False

    @functools.wraps(f)
    def g(*args, **kwargs):
        # TODO: Use logging module instead?
        nonlocal warned_for
        if not warned_for:
            warnings.simplefilter("always", DeprecationWarning)  # turn off filter
            warnings.warn(
                "Call to deprecated function {}, this warning will only show once per function.".format(
                    f.__name__
                ),
                category=DeprecationWarning,
                stacklevel=2,
            )
            warnings.simplefilter("default", DeprecationWarning)  # reset filter
            warned_for = True
        return f(*args, **kwargs)

    return g


def restart_on_exception(f, delay=1, exception=Exception):  # pragma: no cover
    @functools.wraps(f)
    def g(*args, **kwargs):
        while True:
            try:
                f(*args, **kwargs)
            except exception as e:
                # TODO: Use warnings module instead?
                logging.error(
                    "{} crashed due to exception, restarting.".format(f.__name__)
                )
                logging.error(e)
                time.sleep(
                    delay
                )  # To prevent extremely fast restarts in case of bad state.

    return g
