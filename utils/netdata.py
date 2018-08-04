from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import requests
import urllib.request
import logging
import ssl
import random
import time

def download(url, save_fname, params=None, overwrite=False, chunk_size=1024 * 1024 * 16):
    """Download a file from remote site.

    Parameters
    ----------
    url: str
        Url of remote data.

    save_fname: str
        Local file name to save the data.

    params: bytes/dict (optional)
        Dictionary or bytes to be sent in the query string for the :class:`Request`.

    overwrite: boolean
        Whether or not overwrite the file if it already exists.

    chunk_size: integer
        The number of bytes it should read into memory.

    Returns
    -------
    result: boolean
        Result of the download operation.

    file_size: integer
        The size of the downloaded file.

    """
    if os.path.exists(save_fname) and not overwrite:
        logging.info("Target file %s already exists" % save_fname)
        return True, os.path.getsize(save_fname)

    r = requests.get(url, params=params, stream=True)
    if r.status_code != requests.codes.ok:
        return False, 0

    save_fname = os.path.realpath(save_fname)
    save_dir = os.path.dirname(save_fname)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    elif os.path.isfile(save_dir):
        raise IOError('%s is existed and is a file' % (save_dir))

    size = 0
    logging.info("Request %s, download %.2fMB" % (url, 0))
    with open(save_fname, 'wb') as fout:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk: # filter out keep-alive new chunks
                fout.write(chunk)
                fout.flush()
                size += len(chunk)
                logging.info("Request %s, download %.2fMB" % (url, size / 1024. ** 2))
    return True, size


def post(url, files=None, data=None, encoding="utf-8", **kwargs):
    """Upload files to remote site.

    Parameters
    ----------
    url: str
        Url of remote server.

    files: dict
        Files which need to be uploaded, format:
            {
                "remote_field_name_1": local_fname_1,
                "remote_field_name_2": local_fname_2,
            }

    data: dict/bytes (optional)
        Dictionary (will be form-encoded), bytes, or file-like object to send
        in the body of the :class:`Request`.

    kwargs: Dictionary
        Optional arguments that ``request'' takes.

    Returns
    -------
    succeed: boolean
        Result of the upload operation.

    content: str
        The returned content of remote server.

    """
    files_stream = dict()
    if files is not None:
        for field_name, fname in files.items():
            files_stream[field_name] = open(fname, 'rb')

    r = requests.post(url, data=data, files=files_stream, **kwargs)

    for open_file in files_stream.values():
        open_file.close()

    if r.status_code != requests.codes.ok:
        return False, str(r.content, encoding)
    return True, str(r.content, encoding)


def request(url, data=None, use_ssl=False, encoding='utf-8', retry=3, max_sleep_time=60):
    """Fetch the content of a url.

    Parameters
    ----------
    url: str
        Url to fetch.

    data: dict
        Must be an object specifying additional data to be sent to the server,
        or None if no such data is needed.

    use_ssl: boolean
        Whether request the url through a SSL conection or not.

    encoding: str
        Encoding of the url's content. It will first try to use the charset
        specified by the http headers, if there is no specified charset,
        use this parameter instead.

    retry: integer
        Maximum attempt number to request the target url.

    max_sleep_time: integer
        Maximum sleep time in seconds between each attempt.
        It will use a random seconds between [0, max_sleep_time].

    Returns
    -------
    contents: the body of the request

    """
    if use_ssl:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx = None

    last_except = None
    for i in range(retry):
        try:
            if data is not None:
                data = urllib.parse.urlencode(data).encode(),
            r = urllib.request.urlopen(url, data=data, context=ctx)
            raw_data = r.read()
            return str(raw_data, r.headers.get_content_charset(encoding))

        except urllib.error.HTTPError as e:
            last_except = e
            sleep_time = random.randrange(0, max_sleep_time)
            time.sleep(sleep_time)
            logging.warning("Got HTTP Error %s. Sleeping %i seconds and trying again "\
                            "for other %i times", e.code, sleep_time, retry - i - 1)

    logging.error("Error while requesting '%s', data = '%s'" % (url, str(data)))
    raise last_except

