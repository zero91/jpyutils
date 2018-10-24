from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import gzip
import zipfile
import logging
import numpy as np
from jpyutils import utils

class Embeddings(object):
    """Manage word embedding resources.

    Supported Embeddings Resources:
    1. Glove(https://nlp.stanford.edu/projects/glove/)
       a. glove.42B.300d:
          Common Crawl (42B tokens, 1.9M vocab, uncased, 300d vectors, 1.75 GB download)
       b. glove.840B.300d:
          Common Crawl (840B tokens, 2.2M vocab, cased, 300d vectors, 2.03 GB download)
       c. glove.6B: Wikipedia 2014 + Gigaword 5
            (6B tokens, 400K vocab, uncased, 50d, 100d, 200d, & 300d vectors, 822 MB download)

    """
    def __init__(self, local_dir="~/.jpyutils/data/embeddings"):
        """Manage word embedding resources.

        Parameters
        ----------
        local_dir: str
            Local path for saving resources.

        """
        self.__local_dir = os.path.realpath(os.path.expanduser(local_dir))
        self.__resources_info = {
            "glove.42B.300d": {
                "url": "http://nlp.stanford.edu/data/glove.42B.300d.zip",
                300: ".*",
            },
            "glove.840B.300d": {
                "url": "http://nlp.stanford.edu/data/glove.840B.300d.zip",
                300: ".*",
            },
            "glove.6B": {
                "url": "http://nlp.stanford.edu/data/glove.6B.zip",
                50: "glove.6B.50d.txt",
                100: "glove.6B.100d.txt",
                200: "glove.6B.200d.txt",
                300: "glove.6B.300d.txt",
            }
        }

    def load(self, resource_name, dim, vocabulary=None, id_shift=0,
                                       resource_info=None, normalize=True):
        """Load word embeddings resources.

        Parameters
        ----------
        resource_name: str
            Embeddings resource name.

        dim: tuple
            Dimension of the embeddings resource.

        vocabulary: list
            Word candidates for which we need to extract word embeddings.

        id_shift: integer
            Minimum word id for existing word.
            'id_shift' randomized word vectors will be generated,
            and will be appended in the front of the embeddings.

        resource_info: dict
            Word embeddings resource information.
            If not None, it should contains 'url' (for remote file) or
            'local' (for local file)  of the resource
            and at least one dimension with its filelist.


        normalize: boolean
            Set True if you want a normalized embeddings.

        Returns
        -------
        word2id: dict
            Word's id which has embeddings.

        word_embeddings: np.array
            Word's embeddings matrix.

        """
        if resource_info is None:
            if resource_name not in self.__resources_info:
                raise ValueError("Can't find resource info for '%s', you need to specified "\
                        "it by setting value of parameter 'resource_info'" % (resource_name))
            else:
                resource_info = self.__resources_info[resource_name]

        if 'url' in resource_info:
            local_zip = "%s/%s" % (self.__local_dir, resource_name)
            succeed, size = utils.netdata.download(resource_info['url'], local_zip)
            if not succeed or size == 0:
                raise IOError("Download file '%s' failed" % (resource_info['url']))
        elif 'local' in resource_info:
            local_zip = resource_info['local']
        else:
            raise ValueError("The 'resource_info' you specified should at least "\
                             "contains key 'url' or 'local'")

        contents = utils.utilities.read_zip(local_zip, filelist=resource_info[dim], merge=True)

        word2id = dict()
        vocab_set = set(vocabulary) if vocabulary is not None else set()
        vectors = list()
        for line_cnt, line in enumerate(contents.split('\n'), 1):
            if len(line) == 0:
                continue
            fields = line.rsplit(maxsplit=dim)
            if len(fields) != dim + 1:
                logging.warning("dimension of line [%s] is not equal to %d (line_num: %d)" % (
                        line, dim, line_cnt))
                continue

            if len(fields[0]) > 128:
                logging.warning("word [%s] is longer than 128 and will be discarded " \
                        "(line_num: %d)" % (fields[0], line_cnt))
                continue

            if vocabulary is not None and fields[0] not in vocab_set:
                continue

            if fields[0] in word2id:
                logging.warning("vector of word [%s] already exists" % (fields[0]))
                continue

            vectors.append(np.array(list(map(float, fields[1:])), dtype=np.float32))
            word2id[fields[0]] = len(word2id) + id_shift
            if line_cnt % 50000 == 0:
                logging.info("Read %.2fK lines" % (line_cnt / 1000.))

        word_embeddings = np.array(vectors, dtype=np.float32)
        if id_shift > 0:
            word_embeddings = np.append(self.generate((id_shift, dim), normalize=False),
                                        word_embeddings, axis=0)

        if normalize:
            norms = np.linalg.norm(word_embeddings, axis=1).reshape((-1, 1))
            word_embeddings /= norms
        return word2id, word_embeddings

    def generate(self, shape, normalize=True):
        """Generate random embeddings.

        Parameters
        ----------
        shape: tuple
            Target random numpy array, shape (word_num, dimension).

        Returns
        -------
        random_embeddings: np.array
            numpy array with uniform random values in range (-1, 1) of shape 'shape'.

        """
        random_embeddings = np.random.uniform(-1, 1, shape)
        if normalize:
            norms = np.linalg.norm(random_embeddings, axis=1).reshape((-1, 1))
            random_embeddings /= norms
        return random_embeddings

