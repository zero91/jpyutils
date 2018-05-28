from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import random
import string

def random_str(size, chars=string.ascii_letters, sep=''):
    """Randomly generates a string of length `size`.

    Parameters
    ----------
    size: integer
        Length of the generated string.

    chars: str
        The candidate characters of the elements in the string.

    Returns
    -------
    random_string: str
        The randomly generated string of length `size`.

    """
    if not isinstance(chars, str):
        raise TypeError("Parameter `chars` is not a str object")
    return sep.join([random.choice(chars) for i in range(size)])
