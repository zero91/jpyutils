from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import requests
import logging

def download(url, save_fname, overwrite=False, chunk_size=1024 * 1024 * 16):
    """Download a file from remote site.

    Parameters
    ----------
    url: str
        Url of remote data.

    save_fname: str
        Local file name to save the data.

    overwrite: boolean
        Whether or not overwrite the file if it already exists.

    chunk_size: integer
        The number of bytes it should read into memory.

    Returns
    -------
    result: boolean
        Result of the download operation.

    headers: dict
        The headers returned from the remote server.

    """
    if os.path.exists(save_fname) and not overwrite:
        logging.info("Target file %s already exists" % save_fname)
        return True, None

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
    logging.info("Request %s, download %.2fMB" % (url, 0))
    with open(save_fname, 'wb') as fout:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk: # filter out keep-alive new chunks
                fout.write(chunk)
                fout.flush()
                size += len(chunk)
                logging.info("Request %s, download %.2fMB" % (url, size / 1024. ** 2))
    return True, r.headers
