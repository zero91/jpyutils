from lanfang import runner
from lanfang import utils
import re
import tempfile
import glob
import urllib.parse
import os
import logging
import requests
import copy


_g_m_protocol_downloader = {}


def parse_url(url):
  """Parse url of format type+protocal://url

  Parameters
  ----------
  url: string
    The index url.

  Returns
  -------
  protocol: string
    The protocol of the url.

  url: string
    The uri of the resource.

  params: dict
    The parameters of the url.

  """
  parsed = urllib.parse.urlparse(url)
  protocol = parsed.scheme.split('+')[0]
  if '+' in parsed.scheme:
    parsed = urllib.parse.urlparse(url[len(protocol) + 1:])

  params = dict(urllib.parse.parse_qsl(parsed.query))
  for param in ['update', 'verify_ssl', 'no_deps']:
    if param in params:
      params[param] = params[param].lower() == 'true'

  main_url = parsed.scheme + "://" + parsed.netloc + parsed.path.split('?')[0]
  return protocol, main_url, params


def download(uri, save_path, params=None, overwrite=False):
  """Download data adapter.

  Parameters
  ----------
  uri: string
    The uri of the resources. Examples,
      1) https://raw.githubusercontent.com/zero91/data/master/NER/MSRA.zip
      2) pip+https://pypi.org/simple?name=requests&version=2.17.0

  save_path: string
    The destination path.

  params: dict
    The extra paramaters for download the resource.

  overwrite: boolean
    Whether to overwrite the existed data or not.

  Returns
  -------
  data_files: list or string
    If there is only one path, return a string. Otherwise, return a list.

  """
  protocol, url, existed_params = parse_url(uri)
  if params is None:
    params = {}
  else:
    params = copy.deepcopy(params)
  params.update(existed_params)

  if protocol not in _g_m_protocol_downloader:
    raise KeyError("Unsupported protocol %s" % protocol)

  try:
    return _g_m_protocol_downloader[protocol](
        url, save_path, params, overwrite)
  except Exception as e:
    logging.error("An exception occurred while downloading data from %s", uri)
    raise e


def downloader_register(name):
  """Register a new downloader.

  Each downloader mush have signature (save_path, url, params, overwrite).
  """
  def downloader_register_wrapper(downloader):
    names = name
    if not isinstance(name, (list, tuple)):
      names = [name]

    for new_name in names:
      if new_name in _g_m_protocol_downloader:
        raise KeyError("Downloader '%s' already been registered" % new_name)
      _g_m_protocol_downloader[new_name] = downloader
    return downloader
  return downloader_register_wrapper


@downloader_register("pip")
def pip_download(url, save_path, params, overwrite=False):
  """Using pip to download python packages.

  Parameters
  ----------
  url: string
    The index-url for pip.

  save_path: string
    The destination path.

  params: dict
    The paramaters info of the package, should have key 'name'.

  overwrite: boolean
    Whether to overwrite the existed data or not.

  Returns
  -------
  data_files: list or string
    If there is only one path, return a string. Otherwise, return a list.

  Raises
  ------
  ValueError: params does not have key 'name' or version is invalid.

  """
  if 'name' not in params:
    raise ValueError("Package name must be provided")

  name = params['name']
  version = params.get('version', 'latest')
  if not version or version == 'latest':
    version = ''
  elif re.match(r'^\d', version):
    version = '==' + version
  elif not re.match(r'^[=<>]', version):
    raise ValueError("Invalid package version.")
  package_name = name + version

  if params.get('no_deps', True):
    deps = '--no-deps'
  else:
    deps = ''

  tmp_path = tempfile.mkdtemp()

  url = url.strip()
  if url is None or url == "":
    pip_cmd = ["pip", "download", "--dest", tmp_path, package_name, deps]
  else:
    pip_cmd = ["pip", "download", "--dest", tmp_path,"--index-url", url,
                package_name, deps]

  task = runner.TaskRunner(target=pip_cmd, retry=3)
  task.start()
  task.join()

  data_files = glob.glob(os.path.join(tmp_path, '*'))
  if task.exitcode != 0 or len(data_files) == 0:
    logging.warning("pip download data failed")
    return []
  return utils.disk.move_data(data_files, save_path, overwrite)


@downloader_register(["http", "https"])
def http_download(url, save_path, params=None, overwrite=False,
                                  chunk_size=1024 * 1024 * 16):
  """Download a file from remote site.

  Parameters
  ----------
  save_path: str
    Destination path for saving the data.

  url: str
    Url of remote data.

  params: dict (optional)
    Parameters to be sent in the query string for the :class:`Request`.

  overwrite: boolean
    Whether or not overwrite the file if it is already exists.

  chunk_size: integer
    The number of bytes it should read into memory.

  Returns
  -------
  data_file: string
    The local file which saved the remote data.

  """
  r = requests.get(url, params=params, stream=True)
  if r.status_code != requests.codes.ok:
    logging.warning("Download data from '%s' failed with status code %d" % (
            url, r.status_code))
    return None

  remote_fname = os.path.basename(urllib.parse.urlparse(url).path)
  remote_fsize = int(r.headers['Content-Length'])
  if not overwrite:
    if os.path.isfile(save_path) \
        and os.path.getsize(save_path) == remote_fsize:
      return save_path

    dir_save_fname = os.path.join(save_path, remote_fname)
    if os.path.isfile(dir_save_fname) \
        and os.path.getsize(dir_save_fname) == remote_fsize:
      return dir_save_fname

  if os.path.isdir(save_path):
    fout = open(os.path.join(save_path, remote_fname), 'wb')
  else:
    os.makedirs(os.path.dirname(os.path.realpath(save_path)), exist_ok=True)
    fout = open(save_path, 'wb')

  size = 0
  logging.info("Request %s, size %.2fMB, download 0.00MB",
    url, remote_fsize / 1024. ** 2)

  for chunk in r.iter_content(chunk_size=chunk_size):
    if chunk: # filter out keep-alive new chunks
      fout.write(chunk)
      fout.flush()
      size += len(chunk)
      logging.info("Request %s, size %.2fMB, download %.2fMB" % (
          url, remote_fsize / 1024. ** 2, size / 1024. ** 2))
  fout.close()
  return fout.name
