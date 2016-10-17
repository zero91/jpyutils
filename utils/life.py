"""Small functions for daily life's calculation.
"""

# Author: Donald Cheung <jianzhang9102@gmail.com>
from __future__ import division

def present_value(interest_rate, value_list):
    """Set tint for a string.

    Parameters
    ----------
    interest_rate : float
        The interest rate of each time.

    value_list : list
        The value list of each time.

    Returns
    -------
    present_value: float
        The present value of the future value list.

    """
    tot_present = 0.0
    for time, value in enumerate(value_list):
        tot_present += value / ((1 + interest_rate) ** time)
    return tot_present
