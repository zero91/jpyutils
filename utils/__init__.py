"""
Collections of small functions.
===============================
"""

from . import random
from . import utilities
from . import netdata
from . import text
from . import monitor
from . import terminal

try:
    from . import text
    __all__ = ["random", "utilities", "netdata", "text", "monitor", "terminal"]

except Exception as e:
    __all__ = ["random", "utilities", "netdata", "monitor", "terminal"]

"""
from . import check
from . import chinese
from . import data_proxy
from .deprecation import deprecated
from .module_builder import ModuleBuilder
from .simple_conf import SimpleConf
"""
