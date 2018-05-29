from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import requests
import logging

def download(url, save_fname, chunk_size=1024 * 1024 * 16):
    """Download a file from remote site.

    Parameters
    ----------
    url: str
        Url of remote data.

    save_fname: str
        Local file name to save the data.

    chunk_size: integer
        The number of bytes it should read into memory.

    Returns
    -------
    result: boolean
        Result of the download operation.

    headers: dict
        The headers returned from the remote server.

    """
    logger = logging.getLogger()
    r = requests.get(url, stream=True)
    if r.status_code != requests.codes.ok:
        return False, r.headers

    save_fname = os.path.realpath(save_fname)
    save_dir = os.path.dirname(save_fname)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    elif os.path.isfile(save_dir):
        raise IOError('%s is existed and is a file' % (save_dir))

    size = 0
    content_length = float(r.headers['Content-Length'])
    logging.error("Download %s, progress %%%.2f" % (url, size / content_length * 100))
    with open(save_fname, 'wb') as fout:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk: # filter out keep-alive new chunks
                fout.write(chunk)
                fout.flush()
                size += len(chunk)
                logging.error("Download %s, progress %%%.2f" % (url, size / content_length * 100))
    return True, r.headers
