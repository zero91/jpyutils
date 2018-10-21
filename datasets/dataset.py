from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import abc
from .. import internal

class Dataset(abc.ABC):
    """Abstract base class for managing dataset.

    """
    def __init__(self):
        """Constructor.
        """
        self._m_module_conf = internal.utils.load_conf()

    @abc.abstractmethod
    def load(self, dataset=None, tokenizer=None, lowercase=True):
        pass
