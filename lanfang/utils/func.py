import inspect


def extract_kwargs(func, params, *, raises=False, return_missing=False):
  """Extract parameters of a function from input params.

  Parameters
  ----------
  func: callable object
    The callable object needed to extract parameters from.

  params: dict
    The input where contains the values of parameters.

  raises: boolean
    Raise TypeError if the required parameters are not all specified.

  return_missing: boolean
    Return missing parameters is set True.

  Returns
  -------
  kwargs: dict
    The parameters of the callable func.

  Raises
  ------
  TypeError: If some required parameters are missing and raises is set True.
  """

  signature = inspect.signature(func)
  kwargs = {}
  missing_values = []
  for name, param in signature.parameters.items():
    if param.kind == inspect.Parameter.VAR_KEYWORD:
      continue
    if name in params:
      kwargs[name] = params[name]
    elif param.default == inspect.Parameter.empty:
      missing_values.append(name)

  if raises and len(missing_values) > 0:
    raise TypeError("{} required parameter{} of {} {} missing: {}".format(
        len(missing_values),
        "s" if len(missing_values) > 1 else "",
        func.__name__,
        "are" if len(missing_values) > 1 else "is",
        ", ".join(missing_values)))

  if return_missing:
    return kwargs, missing_values
  else:
    return kwargs


def subclasses(root_class, recursive=True):
  """Get all subclasses of a class.

  Parameters
  ----------
  root_class: class
    A python class.

  recursive: bool
    Set True to get all offspring classes, otherwise get all son classes.

  Returns
  -------
  all_subclasses: set
    All subclasses.
  """

  all_subclasses = set()
  for sub_class in root_class.__subclasses__():
    all_subclasses.add(sub_class)
    all_subclasses |= subclasses(sub_class)
  return all_subclasses
