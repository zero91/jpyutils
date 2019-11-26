"""A toolkit to make daily work and learning more efficient.
"""
__version__ = "0.1.6-alpha"

import re as _re
import warnings as _warnings

# Make sure that DeprecationWarning within this package always gets printed
_warnings.filterwarnings('always', category=DeprecationWarning,
                        module=r'^{0}\.'.format(_re.escape(__name__)))
del _re
del _warnings

from lanfang import utils
from lanfang import runner
from lanfang import network
from lanfang import ai
from lanfang import dev


__all__ = [_s for _s in dir() if not _s.startswith('_')]
