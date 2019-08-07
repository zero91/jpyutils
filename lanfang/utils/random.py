import random
import string


def random_str(size, chars=string.ascii_letters, sep=''):
  """Randomly generates a string of length `size`.

  Parameters
  ----------
  size: int
    Length of the generated string.

  chars: str
    The candidate characters of the elements in the string.

  Returns
  -------
  random_string: str
    The randomly generated string of length `size`.

  """
  if not isinstance(chars, (str, tuple, list)):
    raise TypeError("Parameter `chars` is not a seq object")
  return sep.join([random.choice(chars) for _ in range(size)])
