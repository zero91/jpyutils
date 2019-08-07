# coding: utf-8
# Author: Donald Cheung <jianzhang9102@gmail.com>
"""Utility for managing simple <key, value> configuration file.
"""
import os
import collections

class SimpleConf(collections.MutableMapping):
    """Manage simple configuration file.

    It reads configuration file which contains a list of <key: value> config,
    and can be used as a dictionary.

    Parameters
    ----------
    conf_fname: string/basestring
        Path name of configuration file.

    """
    def __init__(self, conf_fname=None):
        if conf_fname is not None and isinstance(conf_fname, basestring):
            self.load_conf(conf_fname)

    def load_conf(self, conf_fname, clear=True):
        """Load config.

        Parameters
        ----------
        conf_fname: string/basestring
            Path name of configuration file.

        clear: boolean
            Whether or not to clear old configuration which had been loaded before.

        """
        if not isinstance(conf_fname, basestring):
            raise TypeError("Parameter `conf_fname` should be type `basestring`")

        if not os.path.isfile(conf_fname):
            raise IOError("File [{0}] doest not exists".format(conf_fname))

        if clear is True:
            self.__dict__.clear()

        key_value_list = list()
        for line in open(conf_fname, 'r'):
            kv = map(str.strip, line.split(':', 1))
            if len(kv) != 2 or len(kv[0]) == 0 or kv[0][0] == '#':
                continue
            key_value_list.append(kv)
        self.__dict__.update(key_value_list)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

