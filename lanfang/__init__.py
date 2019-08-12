"""A toolkit to make daily work and learning more efficient.
"""
__version__ = "0.1.2"

import re
import warnings

# Make sure that DeprecationWarning within this package always gets printed
warnings.filterwarnings('always', category=DeprecationWarning,
                        module=r'^{0}\.'.format(re.escape(__name__)))


from lanfang import utils
from lanfang import runner
from lanfang import network
from lanfang import dev


__all__ = [_s for _s in dir() if not _s.startswith('_')]
