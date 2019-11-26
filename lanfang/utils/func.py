import inspect


def extract_kwargs(func, params, *, raises=False):
  """Extract parameters of a function from input params.

  Parameters
  ----------
  func: callable object
    The callable object needed to extract parameters from.

  params: dict
    The input where contains the values of parameters.

  raises: boolean
    Raise TypeError if the required parameters are not all specified.

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
  return kwargs
