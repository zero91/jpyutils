from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import yaml

def module_path():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def load_conf():
    with open(os.path.join(module_path(), 'conf', 'basic.yaml'), 'r') as fin:
        conf = yaml.safe_load(fin)
        conf['cache_path'] = os.path.realpath(os.path.expanduser(conf['cache_path']))
    return conf

