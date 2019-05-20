"""
Collections of small functions.
===============================
"""

from .deprecation import deprecated
from . import terminal
from . import disk
from . import random
from jpyutils.utils import misc_utils

__all__ = ["deprecated", "terminal", "disk", "random", "misc_utils"]
