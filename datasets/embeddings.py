from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import gzip
import zipfile
import logging
import urllib
import numpy as np

from .dataset import Dataset
from .. import utils

class Embeddings(Dataset):
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
    def __init__(self):
        """Constructor.
        """
        super(self.__class__, self).__init__()
        self._m_dataset_conf = self._m_datasets_conf["embeddings"]
        self._m_dataset_path = os.path.join(self._m_datasets_path, "embeddings")

    def add_data(self, dataset, url, dimension, local=None):
        """Add new embedding data configuration.

        Parameters
        ----------
        dataset: str
            Embeddings dataset name.

        url: str
            The download url of the dataset. Remote type of dataset must be zip file.

        dimension: dict
            Dimension of the embedding data. Format:
                {
                    dim_1: file_pattern_1,
                    dim_2: file_pattern_2,
                }

        local: str
            Local path of the embedding data.
            Set url to None if you want this parameter to take effect.

        Returns
        -------
        succeed: bool
            True if succeed otherwise False.

        """
        if dataset in self._m_dataset_conf:
            raise KeyError("Dataset '%s' already exists" % (dataset))

        if not isinstance(dimension, dict):
            raise TypeError("parameter 'dimension' must be of type 'dict'")

        for dim, file_pattern in dimension.items():
            if not isinstance(dim, int):
                raise TypeError("Key of parameter 'dimension' must be of type 'int'")

        if url is not None:
            self._m_dataset_conf[dataset] = { "url": url }

        elif local is not None:
            self._m_dataset_conf[dataset] = { "local": local }

        else:
            raise ValueError("You should specify the dataset's path " \
                             "by setting the value of 'url' or 'local'")
        self._m_dataset_conf[dataset].update(dimension)
        return True

    def load(self, dataset, dim=300, vocabulary=None, extra_dict=None, normalize=True):
        """Load word embeddings resources.

        Parameters
        ----------
        dataset: str
            Dataset of embeddings.

        dim: tuple
            Dimension of the embeddings resource.

        vocabulary: list
            Word candidates for which we need to extract word embeddings.

        extra_dict: dict
            Extra word added the final dictionary and create ramdom embeddings for the word.
            e.g.
                {
                    "<BEG>": 1000000,
                    "<END>": 1000000,
                }

        normalize: boolean
            Set True if you want a normalized embeddings.

        Returns
        -------
        word2id: dict
            Word's id which has embeddings.

        id2word: dict
            id to word mapping, id => word.

        word_embeddings: np.array
            Word's embeddings matrix.

        """
        if dataset not in self._m_dataset_conf:
            raise KeyError("Can't find dataset '%s'" % (dataset))

        if dim not in self._m_dataset_conf[dataset]:
            raise KeyError("Can't find dimension=%d for dataset '%s'" % (dim, dataset))

        if 'url' in self._m_dataset_conf[dataset]:
            local_zip = os.path.join(
                self._m_dataset_path,
                os.path.basename(urllib.parse.urlparse(self._m_dataset_conf[dataset]['url']).path)
            )
            succeed, size = utils.netdata.download(self._m_dataset_conf[dataset]['url'], local_zip)
            if not succeed or size == 0:
                raise IOError("Download file '%s' failed" % (self._m_dataset_conf[dataset]['url']))

        elif 'local' in self._m_dataset_conf[dataset]:
            local_zip = self._m_dataset_conf[dataset]['local']

        else:
            raise ValueError("You should specify the dataset's path " \
                             "by setting the value of 'url' or 'local'")

        word2id = dict() if extra_dict is None else extra_dict.copy()
        vocab_set = set(vocabulary) if vocabulary is not None else set()
        vectors = list()

        if local_zip.split(".")[-1] == "zip":
            contents = utils.utilities.read_zip(local_zip,
                                                filelist=self._m_dataset_conf[dataset][dim],
                                                merge=True).split('\n')
        elif local_zip.split(".")[-1] == "gz":
            contents = gzip.open(local_zip, "rt")

        else:
            with open(local_zip, 'r') as fin:
                contents = fin.readlines()

        for line_cnt, line in enumerate(contents, 1):
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

            vectors.append(list(map(float, fields[1:])))
            word2id[fields[0]] = len(word2id)
            if line_cnt % 50000 == 0:
                logging.info("Read %.2fK lines" % (line_cnt / 1000.))

        word_embeddings = np.array(vectors, dtype=np.float32)
        if extra_dict is not None:
            word_embeddings = np.append(self.generate((len(extra_dict), dim), normalize=False),
                                        word_embeddings, axis=0)

        if normalize:
            norms = np.linalg.norm(word_embeddings, axis=1).reshape((-1, 1))
            word_embeddings /= norms
        id2word = {v: k for k, v in word2id.items()}
        return word2id, id2word, word_embeddings

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

