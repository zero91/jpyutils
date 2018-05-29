from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import logging
import os
import datetime

def get_logger(name, level=logging.INFO, save_to_disk=False, path="."):
    """Initialize a logger.
     Return a logger with the specified name, creating it if necessary.

    Parameters
    ----------
    name: str
        Logger's name.

    level: enumerator
        The level of the Logger(DEBUG INFO WARNING ERROR CRITICAL).

    save_to_disk: boolean
        Set True if you want to save the logs to disk,
        otherwise it will be printed to sys.stderr.

    path: str
        Saving directory of logs.
        Set `save_to_disk` to be True if you want this parameter go into effect.

    Returns
    -------
    Logger: logging.Logger
        Logging instance with the specified settings.

    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
            "[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s]: %(message)s")

    if save_to_disk:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=False)
        elif os.path.isfile(path):
            raise IOError('%s is existed and is a file' % (path))

        time_stamp = datetime.datetime.now().strftime("%Y%m%d")
        info_fname = os.path.join(path, '{name}_{time}.log'.format(name=name, time=time_stamp))
        info_handler = logging.FileHandler(info_fname)
        info_handler.setFormatter(formatter)
        logger.addHandler(info_handler)

        err_fname = os.path.join(path, '{name}_{time}.log.err'.format(name=name, time=time_stamp))
        err_handler = logging.FileHandler(err_fname)
        err_handler.setFormatter(formatter)
        err_handler.setLevel(logging.ERROR)
        logger.addHandler(err_handler)

    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
