# coding: utf-8
# Author: Donald Cheung <jianzhang9102@gmail.com>
"""Utility for chinese processing.
"""
import os

def is_chinese_char(uchar, encoding='utf-8'):
    """Return True is uchar is a chinese char, otherwise return False.
    If uchar's length does not equal to 1, return False directly.

    Parameters
    ----------
    uchar : string
        The single char to be check.

    encoding : string, optional
        The uchar's encoding, default to be `utf-8'.

    Returns
    -------
    is_chinese_char : boolean
        True is uchar is a single chinese char, otherwise False.

    Examples
    --------
    >>> is_chinese_char("中", encoding='utf-8')
    True

    >>> is_chinese_char("A", encoding='utf-8')
    False

    >>> is_chinese_char("中文", encoding='utf-8')
    False

    """
    if not isinstance(uchar, unicode):
        uchar = uchar.decode(encoding)

    if len(uchar) != 1:
        return False

    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False

 
# def Chinese_word_extraction(content_raw):
# 	chinese_pattern = u"([\u4e00-\u9fa5]+)"
# 	chi_pattern = re.compile(chinese_pattern)
# 	re_data = chi_pattern.findall(content_raw)
# 	content_clean  = ' '.join(re_data)
def contains_chinese(cstring, encoding='utf-8'):
    """Return True is cstring contains chinese chars, otherwise return False.

    Parameters
    ----------
    cstring: string
        The string which need to be process.

    encoding: string, optional
        The cstring's encoding, default to be `utf-8'.

    Returns
    -------
    contains_chinese: boolean
        True is cstring contains chinese chars, otherwise False.

    """
    if not isinstance(cstring, unicode):
        cstring = cstring.decode(encoding)

    for cchar in cstring:
        if is_chinese_char(cchar, encoding):
            return True
    return False


def half2width(cstring, encoding='utf-8'):
    """Transform a half angle string to width angle.

    Parameters
    ----------
    cstring: string
        A string which contains half-angle chars.

    encoding: string, optional
        The cstring's encoding, default to be `utf-8'.

    Returns
    -------
    transformed_string: string
        Width angle string.

    Examples
    --------
    >>> half2width("2", 'utf-8')
    ２

    >>> half2width("12345", 'utf-8')
    １２３４５

    """
    if not isinstance(cstring, unicode):
        cstring = cstring.decode(encoding)

    transformed_list = list()
    for cchar in cstring:
        code = ord(cchar)
        if code == 0x0020: # half-angle=width-angle-0xfee0 except blank space
            code = 0x3000
        elif 0x0020 < code <= 0x7e:
            code += 0xfee0
        transformed_list.append(unichr(code))

    return "".join([c if encoding is None else c.encode(encoding) for c in transformed_list])

 
def width2half(cstring, encoding='utf-8'):
    """Transform a width angle string to half angle.

    Parameters
    ----------
    cstring: string
        A string which contains width-angle chars.

    encoding: string, optional
        The cstring's encoding, default to be `utf-8'.

    Returns
    -------
    transformed_string: string
        Half angle string.

    Examples
    --------
    >>> width2half("２", 'utf-8')
    2

    >>> width2half("１２３４５", 'utf-8')
    12345

    """
    if not isinstance(cstring, unicode):
        cstring = cstring.decode(encoding)

    transformed_list = list()
    for cchar in cstring:
        code = ord(cchar)
        if code == 0x3000:
            code = 0x0020
        elif 0xff01 <= code <= 0xff5e:
            code -= 0xfee0
        transformed_list.append(unichr(code))

    return "".join([c if encoding is None else c.encode(encoding) for c in transformed_list])


class PinYin(object):
    """Transform chinese string to its pinyin.
    """
    def __init__(self, dict_file=None):
        if dict_file is None:
            dict_file = "{0}/data/word.data".format(os.path.dirname(os.path.realpath(__file__)))

        if not os.path.isfile(dict_file):
            raise IOError("file [{0}] does not exist or is not a regular file".format(dict_file))

        self.word_pinyin = dict()
        for line in open(dict_file, 'r').xreadlines():
            key, value = line[:-1].split('\t', 1)
            self.word_pinyin[key] = value

    def transform(self, cstring, encoding='utf-8', tone=False):
        """Return cstring's pinyin.

        Parameters
        ----------
        cstring: string/unicode
            The string which need to be transformed.

        encoding : string, optional
            The uchar's encoding, default to be `utf-8'.

        tone: boolean, optional
            Return pinyin with its tone if set True. Default to be False.

        Returns
        -------
        pinyin: string
            Pinyin representation of input parameter `cstring'.

        """
        origin_type = type(cstring)
        if not isinstance(cstring, unicode):
            cstring = cstring.decode(encoding)

        result = []
        for char in cstring:
            word = '%X' % ord(char)
            if word in self.word_pinyin:
                if tone is True:
                    result.append(self.word_pinyin[word].split()[0].lower())
                else:
                    result.append(self.word_pinyin[word].split()[0][:-1].lower())
            else:
                result.append(origin_type(char))
        return result
