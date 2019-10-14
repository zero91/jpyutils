"""Loading tasks."""
import re
import os
import importlib.util

# what about .pyc (etc)
# we would need to avoid loading the same tasks multiple times
# from '.py', *and* '.pyc'
VALID_MODULE_NAME = re.compile(r'[_a-z]\w*\.py$', re.IGNORECASE)


class TaskLoader(object):
  """
  This class is responsible for loading tasks according to various criteria
  and register all tasks in lanfang.runner.

  """
  def __init__(self):
    super(TaskLoader, self).__init__()

  def load(self, start_dir, pattern='.*task(s?).py'):
    """Load and register all tasks under directory 'start_dir'.

    Parameters
    ----------
    start_dir: str
      The start directory of the tasks.

    pattern: str
      The pattern of the matched file name.

    """
    start_dir = os.path.abspath(start_dir)
    for root, dirs, files in os.walk(start_dir):
      for fname in files:
        if not VALID_MODULE_NAME.match(fname):
          continue
        full_fname = os.path.join(root, fname)
        if not self._match_path(fname, full_fname, pattern):
          continue
        self._import_file(full_fname, start_dir)

  def _match_path(self, path, full_path, pattern):
    # override this method to use alternative matching strategy
    return re.compile(pattern).match(path) is not None

  def _get_name_from_path(self, path, top_level_dir):
    if path == top_level_dir:
      return '.'

    path = os.path.normpath(path)
    if path.lower().endswith('$py.class'):
      path = path[:-9]
    else:
      path = os.path.splitext(path)[0]

    _relpath = os.path.relpath(path, top_level_dir)
    assert not os.path.isabs(_relpath), "Path must be within the project"
    assert not _relpath.startswith('..'), "Path must be within the project"

    name = _relpath.replace(os.path.sep, '.')
    return name

  def _import_file(self, full_fname, start_dir):
    module_name = self._get_name_from_path(full_fname, start_dir)

    module_spec = importlib.util.spec_from_file_location(
        module_name, full_fname)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module
