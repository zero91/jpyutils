import os
import collections


def is_chinese_char(uchar, encoding='utf-8'):
  """Test whether uchar is a chinese character.
  If length of uchar does not equal to 1, return False directly.

  Parameters
  ----------
  uchar : string
    The single char to be checked.

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

  if isinstance(uchar, bytes):
    uchar = uchar.decode(encoding)

  if len(uchar) != 1:
    return False

  if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
    return True
  else:
    return False

 
def contains_chinese(cstring, encoding='utf-8'):
  """Test wheter a string contains chinese chars.

  Parameters
  ----------
  cstring: string
    A string to be checked.

  encoding: string, optional
    Encoding of cstring, default to be `utf-8'.

  Returns
  -------
  contains_chinese: boolean
    True if cstring contains chinese chars, otherwise False.
  """

  if isinstance(cstring, bytes):
    cstring = cstring.decode(encoding)

  for cchar in cstring:
    if is_chinese_char(cchar, encoding):
      return True
  return False


def half2full(cstring, encoding='utf-8'):
  """Transform a half width string to full width.

  Parameters
  ----------
  cstring: string
    A string which contains half-width chars.

  encoding: string, optional
    The encoding of cstring, default to be `utf-8'.

  Returns
  -------
  res: string
    Full width string.

  Examples
  --------
  >>> half2full("2", 'utf-8')
  '２'

  >>> half2full("12345", 'utf-8')
  '１２３４５'
  """

  if isinstance(cstring, bytes):
    cstring = cstring.decode(encoding)

  res = []
  for cchar in cstring:
    code = ord(cchar)
    if code == 0x0020: # half-angle=width-angle-0xfee0 except blank space
      code = 0x3000
    elif 0x0020 < code <= 0x7e:
      code += 0xfee0
    res.append(chr(code))

  return "".join(res)

 
def full2half(cstring, encoding='utf-8'):
  """Transform a full width string to half width.

  Parameters
  ----------
  cstring: string
    A string which contains full width chars.

  encoding: string, optional
    The encoding of cstring, default to be `utf-8'.

  Returns
  -------
  res: string
    Half width string.

  Examples
  --------
  >>> full2half("２", 'utf-8')
  '2'

  >>> full2half("１２３４５", 'utf-8')
  '12345'
  """

  if isinstance(cstring, bytes):
    cstring = cstring.decode(encoding)

  res = []
  for cchar in cstring:
    code = ord(cchar)
    if code == 0x3000:
      code = 0x0020
    elif 0xff01 <= code <= 0xff5e:
      code -= 0xfee0
    res.append(chr(code))

  return "".join(res)


class PinYin(object):
  def __init__(self, pinyin_file=None):
    if pinyin_file is None:
      pinyin_file = "{0}/pinyin.data".format(
          os.path.dirname(os.path.realpath(__file__)))

    if not os.path.exists(pinyin_file):
      raise IOError("PinYin data file {} does not exist.".format(pinyin_file))

    self._m_pinyin = collections.defaultdict(list)
    with open(pinyin_file, 'r') as fdata:
      for line in fdata:
        fields = line.split()
        word = chr(int(fields[0], base=16))
        for candidate in fields[1:]:
          pronunciation = candidate[:-1]
          tone = int(candidate[-1])
          self._m_pinyin[word].append((pronunciation, tone))

  def mark(self, cstring, encoding='utf-8'):
    """Return pinyin of cstring.

    Parameters
    ----------
    cstring: string
      The string to be marked.

    encoding : string, optional
      The encoding of uchar, default to be `utf-8'.

    Returns
    -------
    pinyin: string
      The marked pinyin of cstring.
    """

    if isinstance(cstring, bytes):
      cstring = cstring.decode(encoding)
    return [self._m_pinyin.get(word, word) for word in cstring]
