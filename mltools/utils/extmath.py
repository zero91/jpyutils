""" Extended math utilities.  """
# Author: Donald Cheung <jianzhang9102@gmail.com>

from __future__ import division
#from functools import partial
import warnings

import numpy as np
#from scipy import linalg
#from scipy.sparse import issparse, csr_matrix

#from . import check_random_state
#from .fixes import np_version
#from ._logistic_sigmoid import _log_logistic_sigmoid
#from ..externals.six.moves import xrange
#from .sparsefuncs_fast import csr_row_norms
#from .validation import check_array
#from ...exceptions import NonBLASDotWarning


def stable_cumsum(arr, rtol=1e-05, atol=1e-08):
    """Use high precision for cumsum and check that final value matches sum

    Parameters
    ----------
    arr : array-like
        To be cumulatively summed as flat
    rtol : float
        Relative tolerance, see ``np.allclose``
    atol : float
        Absolute tolerance, see ``np.allclose``
    """
    out = np.cumsum(arr, dtype=np.float64)
    expected = np.sum(arr, dtype=np.float64)
    if not np.allclose(out[-1], expected, rtol=rtol, atol=atol):
        raise RuntimeError('cumsum was found to be unstable: '
                           'its last element does not correspond to sum')
    return out

