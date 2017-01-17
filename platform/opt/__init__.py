"""
Tools for platform opt.
=======================
"""

def __load_librbary():
    import os
    import glob
    import ctypes
    map(ctypes.cdll.LoadLibrary,
        glob.glob("{0}/depend_libs/*".format(os.path.dirname(os.path.realpath(__file__)))))

__load_librbary()


from . import configure
from . import hadoop

__all__ = ['hadoop', 'configure']
