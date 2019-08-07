from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import abc
from .. import internal

class Dataset(abc.ABC):
    """Abstract base class for managing dataset.

    """
    def __init__(self):
        """Constructor.
        """
        self._m_module_conf = internal.utils.load_conf()
        self._m_datasets_conf = self._m_module_conf["datasets"]
        self._m_datasets_path = os.path.join(self._m_module_conf["cache_path"],
                                             self._m_datasets_conf["path"])

    @abc.abstractmethod
    def load(self, dataset, **args):
        pass

