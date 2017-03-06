"""
Tools for platform opt.
=======================
"""

def __load_librbary():
    import os
    import ctypes

    # must be loaded as the following order:
    loading_list = ["libgcc_s.so.1", "libstdc++.so.6", "libboost_python.so.1.61.0"]
    for lib in loading_list:
        ctypes.cdll.LoadLibrary("{0}/depend_libs/{1}".format(
                        os.path.dirname(os.path.realpath(__file__)), lib))

__load_librbary()

from . import configure
from . import hadoop
from .monitor import Monitor

__all__ = ['hadoop', 'configure', 'Monitor']
