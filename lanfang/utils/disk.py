import os
import shutil
import logging
import hashlib
import time
import datetime
import zipfile
import re


def move_data(src_data_files, target_path, overwrite=False):
  """Move multiple data files to target path.

  Parameters
  ----------
  src_data_files: list or str
    A list of data files in string.

  target_path: str
    Target saving path.

  overwrite: boolean
    Whether or not to overwrite the file if the data file is already exists.

  Returns
  -------
  data_files: list or str.
    List of path for data files after move.

  Raises
  ------
  IOError: If the moving operation can't proceed.

  """
  if src_data_files is None or len(src_data_files) == 0:
    return src_data_files

  is_str = False
  if isinstance(src_data_files, str):
    is_str = True
    src_data_files = [src_data_files]

  #target_path = os.path.realpath(target_path)
  if os.path.isfile(target_path):
    if len(src_data_files) > 1:
      raise IOError(
        "Need to move multiple files, but destination is an existed file")

    if overwrite:
      shutil.move(src_data_files[0], target_path)
    elif md5(src_data_files[0]) !=  md5(target_path):
      raise IOError("Target path '%s' is already exists, " \
        "but with difference content" % (target_path))
    return target_path

  else:
    if not os.path.exists(target_path) and len(src_data_files) == 1:
      parent_path = os.path.dirname(target_path)
      if parent_path == "":
        parent_path = "."
      else:
        os.makedirs(parent_path, exist_ok=True)
      target_fname = shutil.move(src_data_files[0], target_path)
      if not is_str:
        target_fname = [target_fname]
      return target_fname

    os.makedirs(target_path, exist_ok=True)
    dest_data_files = set(os.listdir(target_path))
    failed_data_files = list()
    succeed_data_files = list()
    for data_file in src_data_files:
      new_data_file = os.path.join(target_path, os.path.basename(data_file))
      if os.path.exists(new_data_file) and not overwrite \
          and md5(data_file) != md5(new_data_file):
        failed_data_files.append(data_file)
        continue
      succeed_data_files.append(shutil.move(data_file, new_data_file))

    if len(failed_data_files) > 0:
      logging.warning("Failed data files: %s" % (", ".join(failed_data_files)))
      raise IOError("Failed to move %d data files" % len(failed_data_files))

    if is_str and len(succeed_data_files) == 1:
      return succeed_data_files[0]
    return succeed_data_files


def md5(files, chunk_size=4096):
  """Calculate md5 value of a file.

  Parameters
  ----------
  files: list
    The list of data files.

  chunk_size: int [optional]
    Read chuk size data each time to compute md5 value progressively.

  Returns
  -------
  md5_value: string
    The md5 hexdigest of the file content.

  """
  if not isinstance(files, (tuple, list)):
    files = [files]

  hash_md5 = hashlib.md5()
  files = sorted(map(os.path.realpath, files))
  for fname in files:
    with open(fname, "rb") as fin:
      for chunk in iter(lambda: fin.read(chunk_size), b""):
        hash_md5.update(chunk)
  return hash_md5.hexdigest()


def is_fresh(path, days=1):
  """Address the freshness of a path.

  Parameters
  ---------
  path: str
    The path of the file or directory.

  days: float
    Should be created at maximum days before current time.
    The argument may be a floating point number for subday precision.

  Returns
  -------
  is_fresh: boolean
    True if the path is fresh, otherwise False.

  """
  if os.path.exists(path) and \
      (time.time() - os.path.getmtime(path)) < days * 3600 * 24:
    return True
  return False


def get_path_create_time(path, time_format="%Y%m%d_%H%M%S"):
  """Get create time of a path.

  Parameters
  ---------
  path: str
    Target path name.

  time_format: str
    The path time's format.

  Returns
  -------
  create_time: str
    The path create time's format string.

  """
  create_timestamp = os.path.getctime(path)
  return time.strftime(time_format, time.localtime(create_timestamp))


def read_zip(zipfname, filelist=None, encoding='utf-8'):
  """Read zip file.

  Parameters
  ----------
  zipfname: str
    zip file's name.

  filelist: list
    The file pattern list which need to be read.
    Default to be all of the files.

  encoding: str
    Encoding of the files.

  Returns
  -------
  contents: dict
    Every matched files.

  """
  fzip = zipfile.ZipFile(zipfname)
  if filelist is None:
    filelist = fzip.namelist()
  elif isinstance(filelist, str):
    filelist = [filelist]
  pattern_list = list(map(re.compile, filelist))

  contents = {}
  for fname in fzip.namelist():
    if all(map(lambda p: p.match(fname) is None, pattern_list)):
      continue
    logging.info("Extracting %s", fname)
    contents[fname] = fzip.read(fname).decode(encoding)
  return contents


def get_logger(name=None, level=logging.WARNING, save_to_disk=False, path="."):
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
