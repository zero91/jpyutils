from __future__ import absolute_import
import random
import string

def random_str(size, seq=string.letters, sep=''):
    """#TODO documentation
    """
    return sep.join([random.choice(seq) for i in range(size)])
