"""Small functions for manipulating terminal's display effect.
"""


def tint(s, font_color="red", bg_color=None, highlight=False):
  """Set tint for a string.

  Parameters
  ----------
  s : str
    The string to get a tint.

  font_color : string
    String's font color, should be one of the following colors:
      ====================================================================
      "black", "red", "green", "yellow", "blue", "purple", "cyan", "white"
      ====================================================================

  bg_color : string          
    String's background color, should be one of the following colors:
      ====================================================================
      "black", "red", "green", "yellow", "blue", "purple", "cyan", "white"
      ====================================================================

  Returns
  -------
  tint_str : str
    Tinted string.

  """
  code_list = []
  if highlight:
    code_list.append("1")

  if font_color is not None:
    code_list.append(str(_tint_code(font_color, "font")))

  if bg_color is not None:
    code_list.append(str(_tint_code(bg_color, "bg")))

  return "\033[%sm%s\033[0m" % (";".join(code_list), s)


def _tint_code(color, place):
  """Get a color's tint code.

  Parameters
  ----------
  color : string
    Color to be coded
    If place is "font", it should be one of the following colors:
      ====================================================================
      "black", "red", "green", "yellow", "blue", "purple", "cyan", "white"
      ====================================================================

    If place is "bg", it should be one of the following colors:
      ====================================================================
      "black", "red", "green", "yellow", "blue", "purple", "cyan", "white"
      ====================================================================

  place : string          
    Color's place, should be "font" or "bg"

  Returns
  -------
  tint_code: integer
    Color's tint code.

  """
  code_dict = {
    "font": {
      "black"  : 30, 
      "red"  : 31,
      "green"  : 32,
      "yellow" : 33,
      "blue"   : 34,
      "purple" : 35,
      "cyan"   : 36,
      "white"  : 37
    },
    "bg": {
      "black"  : 40,
      "red"  : 41,
      "green"  : 42,
      "yellow" : 43,
      "blue"   : 44,
      "purple" : 45,
      "cyan"   : 46,
      "white"  : 47
    }
  }
  if place not in code_dict:
    raise ValueError('Invalid place %s' % place)

  if color not in code_dict[place]:
    raise ValueError('Invalid color %s for place %s' % (color, place))

  return code_dict[place][color]
