import os
import shutil
import logging
import hashlib
import time
import datetime
import zipfile
import re


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
      raise IOError('%s is existed but is a file' % (path))

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


def move_data(src_data_files, target_path, overwrite=False):
  """Move multiple data files to target path.

  Parameters
  ----------
  src_data_files: list or str
    A list of data files in string.

  target_path: str
    Target saving path.

  overwrite: boolean
    Whether to overwrite the file if the data file is already exists.

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

  if isinstance(src_data_files, str):
    is_str = True
    src_data_files = [src_data_files]
  else:
    is_str = False

  if os.path.isfile(target_path):
    if len(src_data_files) > 1:
      raise IOError("Need to move multiple files, "
                    "but destination is an existed file")

    if not overwrite and md5(src_data_files[0]) != md5(target_path):
      raise IOError("Target path '%s' is already exists, "
                    "but with difference content" % (target_path))

    res_path = shutil.move(src_data_files[0], target_path)
    return res_path if is_str else [res_path]

  elif not os.path.exists(target_path) and len(src_data_files) == 1:
    parent_path = os.path.dirname(os.path.realpath(target_path))
    os.makedirs(parent_path, exist_ok=True)
    res_path = shutil.move(src_data_files[0], target_path)
    return res_path if is_str else [res_path]

  else:
    os.makedirs(target_path, exist_ok=True)
    dest_data_files = set(os.listdir(target_path))
    failed_data_files, succeed_data_files = [], []
    for data_file in src_data_files:
      new_data_file = os.path.join(target_path, os.path.basename(data_file))
      if os.path.exists(new_data_file) \
              and not overwrite and md5(data_file) != md5(new_data_file):
        failed_data_files.append(data_file)
        continue
      succeed_data_files.append(shutil.move(data_file, new_data_file))

    if len(failed_data_files) > 0:
      raise IOError("Failed to move %d data files: %s" % (
          len(failed_data_files), ", ".join(failed_data_files)))

    return succeed_data_files[0] if is_str else succeed_data_files


def md5(files, chunk_size=4096):
  """Calculate md5 value of files.

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

  if os.path.exists(path) and (
          time.time() - os.path.getmtime(path)) < days * 3600 * 24:
    return True
  return False


def get_path_create_time(path, time_format="%Y%m%d-%H%M%S"):
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


def keep_files_with_pattern(path_dir, pattern, max_keep_num,
                            key=None, reverse=True):
  """Keep maximum number of files with pattern in a directory,
  remove files if necessary.

  Parameters
  ----------
  path_dir: str
    The directory to operate.

  pattern: str
    The pattern of the files to operate.

  max_keep_num: int
    The maximum number of files to keep.

  key: callable object
    The key to sort the matched files in order.

  reverse: bool
    Reverse the sorted array if set True.

  Returns
  -------
  remove_files: list
    The files been removed.
  """

  if max_keep_num < 0:
    return []

  match_files = []
  match_pattern = re.compile(pattern)
  for fname in os.listdir(path_dir):
    if not re.match(match_pattern, fname):
      continue
    match_files.append(fname)

  match_files.sort(key=key, reverse=reverse)
  remove_files = []
  for match_file in match_files[max_keep_num: ]:
    os.remove(os.path.join(path_dir, match_file))
    remove_files.append(match_file)
  return remove_files


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
