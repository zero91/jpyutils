# coding: utf-8
# Author: Donald Cheung <jianzhang9102@gmail.com>
"""Utility for chinese processing.
"""

def is_chinese_char(uchar, encoding=None):
    """Return True is uchar is a chinese char, otherwise return False.
    If uchar's length does not equal to 1, return False directly.

    Parameters
    ----------
    uchar : string
        The single char to be check.

    encoding : string
        The uchar's encoding, default to be None.

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
    if encoding is not None:
        uchar = uchar.decode(encoding)
    if len(uchar) != 1:
        return False

    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False

 
def contains_chinese(cstring, encoding=None):
    """Return True is cstring contains chinese chars, otherwise return False.

    Parameters
    ----------
    cstring: string
        The string which need to be process.

    encoding: string
        The cstring's encoding, default to be None.

    Returns
    -------
    contains_chinese: boolean
        True is cstring contains chinese chars, otherwise False.

    """
    if encoding is not None:
        cstring = cstring.decode(encoding)

    for cchar in cstring:
        if is_chinese_char(cchar):
            return True
    return False


def half2width(cstring, encoding=None):
    """Transform a half angle string to width angle.

    Parameters
    ----------
    cstring: string
        A string which contains half-angle chars.

    encoding: string
        The cstring's encoding, default to be None.

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
    if encoding is not None:
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

 
def width2half(cstring, encoding=None):
    """Transform a width angle string to half angle.

    Parameters
    ----------
    cstring: string
        A string which contains width-angle chars.

    encoding: string
        The cstring's encoding, default to be None.

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
    if encoding is not None:
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

