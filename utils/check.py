# coding: utf-8
# Author: Donald Cheung <jianzhang9102@gmail.com>
"""Utility for doing small things quickly.
"""
import re

def is_float(s):
    """Return True if s is a float number, otherwise return False.

    Parameters
    ----------
    s : string
        The string to check

    Returns
    -------
    is_float_num : boolean
        True if s is a float number, otherwise False.

    Examples
    --------
    >>> is_float('1.498e-12')
    True

    >>> is_float('1.498e-1.2')
    True

    """
    return re.match(re.compile("^[-+]?[0-9]+(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?$"), s) is not None

