from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .dataset import Dataset
from ..utils import netdata
from ..utils import utilities

import os
import nltk
import json
import logging

class SNLIDataset(Dataset):
    """Managing dataset of SNLI

    SNLI: The Stanford Natural Language Inference (SNLI) Corpus
    Links: https://nlp.stanford.edu/projects/snli/

    """
    def __init__(self, url="https://nlp.stanford.edu/projects/snli/snli_1.0.zip",
                       local_dir="~/workspace/data/datasets"):
        """Constructor

        Parameters
        ----------
        url: str
            Download url of SNLI dataset.

        local_dir: str
            Local saving directory for this dataset.

        """
        self.__url = url
        self.__local_dir = os.path.realpath(os.path.expanduser(local_dir))
        self.__zip_fname = "snli"
        self.__model_data = dict()
        self.__model_data_types = ['train', 'dev', 'test']

    def load(self, dataset='sent_pair', tokenizer=None, lowercase=True):
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

        """
        for model_data_type in self.__model_data_types:
            if model_data_type not in self.__model_data:
                logging.info("Loading source data for %s" % (model_data_type))
                self._load_source_data(model_data_type)

        if dataset == 'sent_pair':
            return self._load_sent_pair(tokenizer, lowercase)
        return None

    def _load_source_data(self, model_data_type):
        local_fname = os.path.join(self.__local_dir, self.__zip_fname)
        netdata.download(self.__url, local_fname)
        self.__model_data[model_data_type] = \
                utilities.read_zip(local_fname, ".*%s\.jsonl"% (model_data_type))

    def _load_sent_pair(self, tokenizer, lowercase):
        sent_pair_data = dict()
        for model_data_type in self.__model_data_types:
            logging.info("Parse data %s" % (model_data_type))
            data = list()
            for line in self.__model_data[model_data_type].split('\n'):
                if line == "":
                    continue
                if lowercase:
                    line = line.lower()
                line_data = json.loads(line)
                label = line_data['gold_label']
                if label == '-':
                    continue

                if tokenizer is not None:
                    tokens1 = tokenizer.tokenize(line_data['sentences1'])
                    tokens2 = tokenizer.tokenize(line_data['sentences2'])
                else:
                    tokens1 = nltk.Tree.fromstring(line_data['sentence1_parse']).leaves()
                    tokens2 = nltk.Tree.fromstring(line_data['sentence2_parse']).leaves()
                data.append((tokens1, tokens2, label))
            sent_pair_data[model_data_type] = data
        return sent_pair_data

