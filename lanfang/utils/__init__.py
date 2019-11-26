"""
Collections of utility functions.
"""

from lanfang.utils.deprecation import deprecated
from lanfang.utils import terminal
from lanfang.utils import disk
from lanfang.utils import random
from lanfang.utils import func


__all__ = [_s for _s in dir() if not _s.startswith('_')]
