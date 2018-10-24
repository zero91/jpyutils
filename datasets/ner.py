from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .dataset import Dataset
from ..utils import netdata
from ..utils import utilities
from .. import mltools
import os
import urllib.parse
import collections
import logging
import operator

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

    def load(self, dataset, **args):
        """Load 'dataset' for future processing.

        Parameters
        ----------
        dataset: str
            Dataset which can be used to do some research.
            Current supported dataset are 'MSRA' and 'CoNLL2003'.

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
        if dataset.lower() == "MSRA".lower():
            return self._load_MSRA()

        elif dataset.lower() == "CoNLL2003".lower():
            return self._load_CoNLL2003()

        raise ValueError("Can't find data for '%s'" % (dataset))

    def _load_MSRA(self):
        logging.info("Loading NER/MSRA data...")
        save_fname = os.path.join(
            self._m_dataset_path,
            os.path.basename(urllib.parse.urlparse(self._m_dataset_conf["MSRA"]).path)
        )
        succeed, _ =  netdata.download(self._m_dataset_conf["MSRA"], save_fname)
        if not succeed:
            raise IOError("Download '%s' failed" % (self._m_dataset_conf['MSRA']))

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

    def _load_CoNLL2003(self):
        logging.info("Loading NER/CoNLL2003 data...")
        save_fname = os.path.join(
            self._m_dataset_path,
            os.path.basename(urllib.parse.urlparse(self._m_dataset_conf["CoNLL2003"]).path)
        )
        succeed, _ =  netdata.download(self._m_dataset_conf["CoNLL2003"], save_fname)
        if not succeed:
            raise IOError("Download '%s' failed" % (self._m_dataset_conf['MSRA']))

        conll2003_ner_data = dict()
        for data_type, data_fname in [("train", "eng.train"),
                                      ("dev", "eng.testa"),
                                      ("test", "eng.testb")]:
            conll2003_ner_data[data_type] = list()
            sentence = list()
            for line in utilities.read_zip(save_fname, data_fname).split('\n'):
                fields = line.strip().split()
                if len(fields) != 4:
                    if len(sentence) > 0 and sentence[0][0] != "-DOCSTART-":
                        conll2003_ner_data[data_type].append(self._convert_tags(sentence))
                    sentence = list()
                else:
                    word, pos_tag, chunk_tag, ner_tag = fields
                    sentence.append((word, ner_tag))
            if len(sentence) > 0 and sentence[0][0] != "-DOCSTART-":
                conll2003_ner_data[data_type].append(sself._convert_tags(entence))
        logging.info("Load NER/CoNLL2003 data succeed!")
        return conll2003_ner_data

    def _convert_tags(self, sentence):
        tags = list(map(operator.itemgetter(1), sentence))
        if not mltools.utils.tags.iob1_to_iob2(tags):
            raise ValueError("Invalid tags: %s"% (sentence))
        return list(map(lambda wt, new_tag: (wt[0], new_tag), sentence, tags))

