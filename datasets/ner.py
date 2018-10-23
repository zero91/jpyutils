from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .dataset import Dataset
from ..utils import netdata
from ..utils import utilities
import os
import urllib.parse
import collections
import logging

class NERDataset(Dataset):
    """Name Entity Recognition Dataset.

    """
    def __init__(self):
        """Constructor
        """
        super(self.__class__, self).__init__()
        self._m_dataset_conf = self._m_datasets_conf["NER"]
        self._m_dataset_path = os.path.join(self._m_datasets_path, "NER")
        self._m_pos2entity = {"nr": "PER", "ns": "LOC", "nt": "ORG"}

    def load(self, dataset, tokenizer=None, lowercase=True):
        """Load 'dataset' for future processing.

        Parameters
        ----------
        dataset: str
            Dataset which can be used to do some research.
            Current supported dataset is 'sent_pair'.

        tokenizer: Tokenizer
            Tokenizer which should has a 'tokenize' method that
            can tokenize a sentence into tokens.

        lowercase: boolean
            Set True if you want to lower all the characters.

        Returns
        -------
        data: dict
            Dictionary for data, have key 'train' for training data,
            'dev' for development data, 'test' for testing data.

        """
        if dataset.lower() == "msra":
            return self._load_MSRA()

        raise ValueError("Can't find data for '%s'" % (dataset))

    def _load_MSRA(self):
        logging.info("Loading NER/MSRA data...")
        save_fname = os.path.basename(urllib.parse.urlparse(self._m_dataset_conf["MSRA"]).path)
        save_fname = os.path.join(self._m_dataset_path, save_fname)
        succeed, _ =  netdata.download(self._m_dataset_conf["MSRA"], save_fname)

        msra_ner_data = dict()
        # O, B-PER, I-PER, B-LOC, I-LOC, B-ORG, I-ORG
        for data_type, data_fname in [("train", "train1.txt"),
                                      ("dev", None),
                                      ("test", "testright1")]:
            if data_fname is None:
                msra_ner_data[data_type] = None
                continue

            msra_ner_data[data_type] = list()
            for line in utilities.read_zip(save_fname, data_fname).split('\n'):
                if len(line.strip()) == 0:
                    continue
                sentence = list()
                for word_tag in line.strip().split():
                    word, pos_tag = word_tag.split('/')
                    for i, char in enumerate(word):
                        if pos_tag in self._m_pos2entity:
                            if i == 0:
                                sentence.append((char, "B-" + self._m_pos2entity[pos_tag]))
                            else:
                                sentence.append((char, "I-" + self._m_pos2entity[pos_tag]))
                        else:
                            sentence.append((char, pos_tag.upper()))
                msra_ner_data[data_type].append(sentence)
        logging.info("Load NER/MSRA data succeed!")
        return msra_ner_data

