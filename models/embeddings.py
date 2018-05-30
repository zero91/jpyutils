"""Manage word embedding resources.

Supported Embeddings Resources:
1. Glove(https://nlp.stanford.edu/projects/glove/)
   a. glove.42B.300d: Common Crawl (42B tokens, 1.9M vocab, uncased, 300d vectors, 1.75 GB download)
   b. glove.840B.300d: Common Crawl (840B tokens, 2.2M vocab, cased, 300d vectors, 2.03 GB download)
   c. glove.6B: Wikipedia 2014 + Gigaword 5
                (6B tokens, 400K vocab, uncased, 50d, 100d, 200d, & 300d vectors, 822 MB download)
"""
import os
import gzip
import zipfile
import logging
import numpy as np
from jpyutils import utils

class Embeddings(object):
    def __init__(self, local_path="~/workspace/data/resources"):
        """Manage word embedding resources.

        Parameters
        ----------
        local_path: str
            Local path for saving resources.

        """
        self.__local_path = os.path.realpath(os.path.expanduser(local_path))
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
            Minimum word id.

        resource_info: dict
            Word embeddings resource information.
            If not None, it should contains 'url' of the resource
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

        local_zip = "%s/%s" % (self.__local_path, resource_name)
        utils.netdata.download(resource_info['url'], local_zip)
        contents = utils.utilities.read_zip(local_zip, filelist=resource_info[dim], merge=True)

        word2id = dict()
        vocab_set = set(vocabulary) if vocabulary is not None else set()
        vectors = list()
        for line_cnt, line in enumerate(contents.split('\n'), 1):
            if len(line) == 0:
                continue
            fields = line.rsplit(maxsplit=dim)
            if len(fields) > 128:
                logging.warning("word [%s] is longer than 128 and will be discarded" % (fields[0]))
                continue
            if vocabulary is not None and fields[0] not in vocab_set:
                continue

            vectors.append(np.array(list(map(float, fields[1:])), dtype=np.float32))
            word2id[fields[0]] = len(word2id) + id_shift
            if line_cnt % 50000 == 0:
                logging.info("Read %.2fK lines" % (line_cnt / 1000.))

        word_embeddings = np.array(vectors, dtype=np.float32)
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
