from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import abc

class Dataset(abc.ABC):
    def __init__(self, url=None, local_dir=None):
        pass

    """Abstract base class for managing dataset.
    """
    @abc.abstractmethod
    def load(self, dataset=None, tokenizer=None, lowercase=True):
        pass
