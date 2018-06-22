from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import logging
import os
import datetime
import zipfile
import operator
import re
import time

def get_logger(name=None, level=logging.INFO, save_to_disk=False, path="."):
    """Initialize a logger.
    Return a logger with the specified name, creating it if necessary.

    Parameters
    ----------
    name: str
        Logger's name. If no name is specified, use the root logger.

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
        info_fname = os.path.join(path, '{name}_{time}.log'.format(
                name=logger.name, time=time_stamp))
        info_handler = logging.FileHandler(info_fname)
        info_handler.setFormatter(formatter)
        logger.addHandler(info_handler)

        err_fname = os.path.join(path, '{name}_{time}.log.err'.format(
                name=logger.name, time=time_stamp))
        err_handler = logging.FileHandler(err_fname)
        err_handler.setFormatter(formatter)
        err_handler.setLevel(logging.ERROR)
        logger.addHandler(err_handler)

    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    handler_fname_set = set()
    for handler in logger.handlers[:]:
        if handler.stream.name in handler_fname_set:
            logger.removeHandler(handler)
            continue
        handler_fname_set.add(handler.stream.name)
    return logger


def read_zip(zipfname, filelist=None, merge=False, encoding='utf-8', sep='\n'):
    """Read zip file.

    Parameters
    ----------
    zipfname: str
        zip file's name.

    filelist: list
        The file list which need to be read. Default to be all of the files.

    merge: boolean
        Whether or not to merge all the file contents.

    encoding: str
        Encoding of the files.

    sep: str
        If merge is True, 'sep' will be used to separate the contents of each file,
        and if file's last character equals to sep, ignore it.

    Returns
    -------
    contents: str/dict
        File contents of for the specified file list.

    """
    fzip = zipfile.ZipFile(zipfname)
    if filelist is None:
        filelist = fzip.namelist()
    elif isinstance(filelist, str):
        filelist = [filelist]
    pattern_list = list(map(re.compile, filelist))

    contents = dict()
    for fname in fzip.namelist():
        if all(map(lambda p: p.match(fname) is None, pattern_list)):
            continue

        logging.info("Extracting %s" % (fname))
        contents[fname] = fzip.read(fname).decode(encoding)
        if merge is True and sep is not None \
                and len(contents[fname]) > 0 and contents[fname][-1] != sep:
            contents[fname][-1] += sep

    if len(contents) <= 1 or merge is True:
        return "".join(contents.values())
    return contents


def is_fresh_file(fname, days=1):
    """Address the freshness of a file.

    Parameters
    ---------
    fname: str
        File's name.

    days: float
        Maximum days before current time. The argument may be a floating point number
        for subday precision.

    Returns
    -------
    is_fresh: boolean
        True if the file is fresh, otherwise False.

    """
    if os.path.exists(fname) and (time.time() - os.path.getmtime(fname)) < days * 3600 * 24:
        return True
    return False

