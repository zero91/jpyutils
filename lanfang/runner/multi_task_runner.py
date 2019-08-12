from lanfang.runner.common import TaskStatus
from lanfang.runner.task_runner import TaskRunner
from lanfang.runner.proc_runner import ProcRunner
from lanfang.runner.progress import TableDisplay
from lanfang.runner.dependency import DynamicTopologicalGraph

import time
import signal
import re
import os
import collections
import copy
import json
import logging
import hashlib
import datetime
import multiprocessing
import argparse


class MultiTaskParams(object):
  """Shared parameters between multiple processes"""
  __GLOBAL_KEY__ = "__parameters__"

  def __init__(self, params, global_params=None, global_key=__GLOBAL_KEY__):
    """initializer.

    Parameters
    ----------
    params: dict, filename
      All parameters of all the tasks beging shared.

    global_params: dict [optional]
      The global parameters to share.

    global_key: str
      The key name to access global parameters.
    """
    self._m_global_key = global_key

    if isinstance(params, dict):
      self._m_params = copy.deepcopy(params)
    elif isinstance(params, str):
      with open(params, 'r') as fin:
        self._m_params = json.load(fin)
    else:
      raise TypeError(
        "Parameter 'params' must be a dict or a string for file name")

    if self._m_global_key not in self._m_params:
      self._m_params[self._m_global_key] = {}

    if global_params is not None:
      if isinstance(global_params, dict):
        self._m_params[self._m_global_key].update(global_params)
      else:
        raise TypeError("Parameter 'global_params' is not a dict")

    self._m_manager = multiprocessing.Manager()
    self._m_share_params = self._m_manager.dict()
    self._m_share_params_lock = multiprocessing.Lock()
    self._m_share_params_hash = None
    self._render_params2share()

  @property
  def share_params(self):
    return self._m_share_params

  @property
  def share_params_lock(self):
    return self._m_share_params_lock

  def update(self):
    self._m_share_params_lock.acquire()
    try:
      new_share_params_hash = self.__share_params_hashcode()
      if new_share_params_hash == self._m_share_params_hash:
        return
      self._render_share2params()
      self._render_params2share()
    finally:
      self._m_share_params_lock.release()

  def dump(self, fname, debug=False):
    fname = os.path.realpath(fname)
    save_path = os.path.dirname(fname)
    if not os.path.exists(save_path):
      os.makedirs(save_path)

    self.update()
    if debug is True:
      params = self._m_share_params.copy()
    else:
      params = self._m_params

    with open(fname, 'w') as fout:
      json.dump(params, fout, sort_keys=True, indent=2)

  def _render_params2share(self):
    # Output
    output_dict = {}
    share_params = {}
    for name, params in self._m_params.items():
      if name == self._m_global_key:
        continue
      share_params[name] = {"output": copy.deepcopy(params["output"])}
      for item_name, item_value in params["output"].items():
        output_dict[name + "." + item_name] = item_value

    # Input
    for name, params in self._m_params.items():
      if name == self._m_global_key:
        continue
      share_params[name]["input"] = {}
      for item, item_scope in params["input"].items():
        if not isinstance(item_scope, list):
          share_params[name]["input"][item] = \
              self.__get_params_item(item_scope, output_dict)
        else:
          item_list = list()
          for scope in item_scope:
            item_list.append(self.__get_params_item(scope, output_dict))
          share_params[name]["input"][item] = item_list

    self._m_share_params.clear()
    self._m_share_params.update(share_params)
    self._m_share_params_hash = self.__share_params_hashcode()

  def _render_share2params(self):
    share_params = self._m_share_params.copy()
    for name, params in self._m_params.items():
      if name == self._m_global_key or name not in share_params:
        continue

      if not isinstance(share_params[name]["output"], dict):
        logging.warning("Task '%s' did not return a dict, it returned [%s]",
            name, share_params[name]["output"])
        continue

      for item in params["output"]:
        if item not in share_params[name]["output"]:
          logging.warning("Task '%s' did not output expected item [%s]",
            name, item)
          continue
        self._m_params[name]["output"][item] = \
            share_params[name]["output"][item]
        share_params[name]["output"].pop(item)

      if len(share_params[name]["output"]) > 0:
        logging.warning("Task '%s' output unexpected values [%s]",
          name, json.dumps(share_params[name]["output"]))

  def __get_params_item(self, scope, output_dict=None):
    try:
      scope = scope.strip()
      if output_dict is not None and scope in output_dict:
        return output_dict[scope]

      name_scope_list = scope.split('.')
      if len(name_scope_list) == 0 or name_scope_list[0] != self._m_global_key:
        name_scope_list.insert(0, self._m_global_key)

      params = self._m_params
      for name_scope in name_scope_list:
        params = params[name_scope]
      return copy.deepcopy(params)
    except Exception as e:
      return copy.deepcopy(scope)

  def __share_params_hashcode(self):
    share_params_str = json.dumps(self._m_share_params.copy(), sort_keys=True)
    return hashlib.md5(share_params_str.encode("utf-8")).hexdigest()


class ArgumentParser(argparse.ArgumentParser):
  """A wrapper class of argparse.ArgumentParser
  which parse parameters from environment variables.
  """

  def __init__(self, **params):
    """Receive the same parameters as argparse.ArgumentParser"""
    super(self.__class__, self).__init__(**params)

  def parse_args(self, args=None, namespace=None):
    args = super(self.__class__, self).parse_args()

    params = os.environ.get("TASK_RUNNER_PARAMETERS")
    if params is not None:
      params = json.loads(params)
      if instance(params, dict):
        raise ValueError(
          "Environment variable 'TASK_RUNNER_PARAMETERS' must be a dict.")
      for key, value in params.items():
        setattr(args, key, value)
    return args


class MultiTaskParamsAnalyzer(object):
  """TODO: Shared parameters between multiple processes need to be
  edited by hand for programmers. It's an awful experience!
  Need to provide some tools to easy this progress.
  """
  def __init__(self, signature=None):
    signature = {
      "train.input.schema_uri": "evaluate.output.uri",
    }

  def add_cmd(self, cmd):
    pass

  def add_func(self, target):
    pass


class PreHookCallback(object):
  """TODO: Add input parameters type check"""
  pass


class PostHookCallback(object):
  """TODO: Add result check"""
  pass


class MultiTaskRunner(object):
  """Run a bunch of tasks.

  It maintains a bunch of tasks, and run the tasks according to
  their topological relations.

  Parameters
  ----------
  log_path: str
    The directory which used to save logs.

    Default to be None, the process's file handles will be inherited
    from the parent.

    Log will be write to filename specified by each task's name
    end up with "out", "err".

  parallel_degree: integer
    Parallel degree of this task.
    At most 'parallel_degree' of the tasks will be executed simultaneously.

  retry: integer
    Try executing each task retry times until succeed.

  interval: float
    Interval time between each try.

  params: str, dict
    Configure for these tasks.

  global_params: dict
    Global parameters for these bunch of tasks.

  render_arguments: dict
    Dict which used to replace tasks parameter for its true value.
    Used for add task from string.

  params_max_checkpoint_num: int
    The maximum number of params checkpoints to save.

  displayer: class
    Class which can display tasks information.

  """
  def __init__(self, log_path=None, parallel_degree=-1, retry=1, interval=5,
                     params=None, global_params=None, render_arguments=None,
                     params_max_checkpoint_num=3, displayer=None):
    if log_path is not None:
      self._m_log_path = os.path.realpath(log_path)
    else:
      self._m_log_path = None

    self._m_parallel_degree = parallel_degree
    self._m_retry = retry
    self._m_interval = interval

    if render_arguments is None:
      self._m_render_arguments = {}
    elif isinstance(render_arguments, dict):
      self._m_render_arguments = render_arguments
    else:
      raise ValueError("Parameter 'render_arguments' should be a dictionary")
    self._m_render_arg_pattern = re.compile(r"\<\%=(.*?)\%\>")

    self._m_dependency = DynamicTopologicalGraph()
    self._m_open_file_list = []
    self._m_runner_dict = collections.defaultdict(dict)
    self._m_running_tasks = set()
    self._m_started = False

    if params is not None:
      self._m_params = MultiTaskParams(params, global_params)
    else:
      self._m_params = None
    self._m_params_max_checkpoint_num = params_max_checkpoint_num

    if displayer is None:
      self._m_displayer_class = TableDisplay
    else:
      self._m_displayer_class = displayer

  def __del__(self):
    for f in self._m_open_file_list:
      f.close()

  def add(self, target, name=None, args=(), kwargs={},
                        pre_hook=None, post_hook=None,
                        depends=None, encoding="utf-8", daemon=True,
                        append_log=False, **popen_kwargs):
    """Add a new task.

    Parameters
    ----------
    target: list, str, callable object
      The target which needs to be executed.

    name: str
      The name of the task. Best using naming method of programming languages,
      for it may be used for creating log files on disk.

    args: tuple
      The argument tuple for the target invocation. Defaults to ().

    kwargs: dict
      The dictionary of keyword arguments for the target invocation.
      Defaults to {}.

    pre_hook: callable
      A callable object to be invoked before 'target' execution.
      The input parameters is the same as 'target'.

    post_hook: callable
      A callable object to be invoked after 'target' execution.
      The input parameters is the return value of calling 'target'.

    depends: str, list, set, dict
      List of depended tasks.
      If this is a string, multiple tasks can be separated by a single comma.

    encoding: str
      The encoding of the data output by target's stdout.

    daemon: boolean
      A boolean value indicating whether this runner
      is a daemon process (True) or not (False).

    append_log: boolean
      Append logs to log file if set True, otherwise clear old content.

    popen_kwargs: dict
      It has the same meaning as arguments of Popen.

    Returns
    -------
    instance: MultiTaskRunner
      Reference for current instance.

    Raises
    ------
    KeyError: If the task is already exists.

    """
    if name in self._m_runner_dict:
      raise KeyError("Task {0} is already exists!".format(name))

    if self._m_log_path is not None:
      logs_path = os.path.join(self._m_log_path, "logs")
      if not os.path.exists(logs_path):
        os.makedirs(logs_path)

      for sname, suffix in [("stdout", "out"), ("stderr", "err")]:
        if sname in popen_kwargs:
          logging.warning("Parameter '%s' already exists for task '%s'",
              sname, name)
          continue
        open_tag = 'a+' if append_log is True else 'w+'
        log_fname = "%s/%s.%s" % (logs_path, name, suffix)
        popen_kwargs[sname] = open(log_fname, open_tag)
        self._m_open_file_list.append(popen_kwargs[sname])

    share_params = None
    share_params_lock = None
    if self._m_params is not None:
      share_params = self._m_params.share_params
      share_params_lock = self._m_params.share_params_lock

    if callable(target):
      runner = ProcRunner(
        target, name=name, args=args, kwargs=kwargs,
        retry=self._m_retry,
        interval=self._m_interval,
        daemon=daemon,
        pre_hook=pre_hook,
        post_hook=post_hook,
        share_dict=share_params,
        share_dict_lock=share_params_lock,
        **popen_kwargs)
    else:
      runner = TaskRunner(
        target, name=name, retry=self._m_retry,
        interval=self._m_interval,
        daemon=daemon,
        pre_hook=pre_hook,
        post_hook=post_hook,
        share_dict=share_params,
        share_dict_lock=share_params_lock,
        encoding=encoding,
        **popen_kwargs)

    self._m_runner_dict[runner.name] = {
      "status": TaskStatus.WAITING,
      "runner": runner,
    }
    self._m_dependency.add(runner.name, depends)
    return self

  def adds(self, tasks_str):
    """Add tasks from a string.

    Parameters
    ----------
    tasks_str: str
      The string of the tasks, which is a python executable code.

    Returns
    -------
    instance: MultiTaskRunner
      Reference for current instance.

    Raises
    ------
    KeyError: Some of the arguments specified in 'tasks_str' did not provided.

    """
    exec(self._render_arguments(tasks_str), {}, {'Runner': self.add})
    return self

  def addf(self, tasks_fname, encoding="utf-8"):
    """Add tasks from a file.

    Parameters
    ----------
    tasks_fname: str
      The file's name of which contains tasks.

    encoding: str
      The encode of file content specified by `tasks_fname'.

    Returns
    -------
    instance: MultiTaskRunner
      Reference for current instance.

    """
    with open(tasks_fname, mode='r', encoding=encoding) as ftask:
      return self.adds(self._render_arguments(ftask.read()))

  def lists(self, display=True):
    """List all tasks.

    Parameters
    ----------
    display: boolean
      Set True if you want to display the tasks info on the screen.

    Returns
    -------
    tasks: list
      The list of tasks in order of running id.

    Raises
    ------
    ValueError: If the tasks relations is not topological.

    """
    if not self._m_dependency.is_valid():
      raise ValueError("The dependency relations of tasks is not topological")

    if display is True:
      self._m_displayer_class(
        self._m_dependency, self._m_runner_dict).write()
    return self._m_dependency.get_nodes(order=True)

  def run(self, tasks=None, verbose=False, try_best=False):
    """Run a bunch of tasks.

    Parameters
    ----------
    tasks: str
      The tasks which needed to be executed,
      see more details of 'DependencyManager.subset'.

    verbose: boolean
      Print verbose information.

    try_best: boolean
      Set true if you want to executed the tasks as many as possible
      even if there exist some failed tasks.

    Returns
    -------
    result : integer
      0 for success, otherwise nonzero.

    Raises
    ------
    RuntimeError:
      If the set of tasks is not topological or has been exected already.

    Notes
    -----
    Should only be executed only once.
    """

    if self._m_started:
      raise RuntimeError("cannot run %s twice" % (self.__class__.__name__))

    if not self._m_dependency.is_valid():
      raise RuntimeError("Dependency relations of tasks is not topological")

    self._m_started = True
    signal.signal(signal.SIGINT, self.__kill_signal_handler)
    signal.signal(signal.SIGTERM, self.__kill_signal_handler)

    run_dependency = self._m_dependency.subset(tasks)
    run_nodes = set(run_dependency.get_nodes())
    for task_name in self._m_runner_dict:
      if task_name not in run_nodes:
        self._m_runner_dict[task_name]["status"] = TaskStatus.DISABLED

    if verbose:
      disp = self._m_displayer_class(self._m_dependency, self._m_runner_dict)
      disp.write()
    else:
      disp = None

    succeed_tasks = set()
    failed_tasks = set()
    while True:
      for task_name in run_dependency.top():
        if self._m_runner_dict[task_name]["status"] not in [
            TaskStatus.READY, TaskStatus.WAITING]:
          continue
        if self._m_parallel_degree < 0 \
            or len(self._m_running_tasks) < self._m_parallel_degree:
          self._m_running_tasks.add(task_name)
          self._m_runner_dict[task_name]["status"] = TaskStatus.RUNNING
          self._m_runner_dict[task_name]["runner"].start()
        else:
          self._m_runner_dict[task_name]["status"] = TaskStatus.READY

      for task_name in self._m_running_tasks.copy():
        if self._m_runner_dict[task_name]["runner"].is_alive():
          continue
        self._m_running_tasks.remove(task_name)

        exitcode = self._m_runner_dict[task_name]["runner"].exitcode
        if exitcode != 0:
          self._m_runner_dict[task_name]["status"] = TaskStatus.FAILED
          logging.critical("Task %s failed, exit code %d", task_name, exitcode)

          failed_tasks.add(task_name)
          offspring = run_dependency.reverse_depends(task_name, recursive=True)
          failed_tasks |= offspring
          for name in offspring:
            self._m_runner_dict[name]["status"] = TaskStatus.CANCELED
        else:
          self._m_runner_dict[task_name]["status"] = TaskStatus.DONE
          if self._m_params is not None:
            self._m_params.update()
          succeed_tasks.add(task_name)
          run_dependency.remove(task_name)

      verbose and disp.write()
      if len(succeed_tasks) + len(failed_tasks) == len(run_nodes):
        break
      if not try_best and len(failed_tasks) > 0:
        self.terminate()
        break
      time.sleep(0.1)

    self._dump_run_params()
    verbose and disp.write(refresh=True)
    return 0

  def __kill_signal_handler(self, signum, stack):
    self.terminate()
    self._m_displayer_class(self._m_dependency, self._m_runner_dict).write()
    logging.info("Receive signal %d, "\
      "all running processes are killed.", signum)
    exit(1)

  def terminate(self):
    """Terminate all running processes."""
    for task_name in self._m_running_tasks.copy():
      try:
        self._m_runner_dict[task_name]["runner"].terminate()
        self._m_runner_dict[task_name]["status"] = TaskStatus.KILLED
      finally:
        self._m_running_tasks.remove(task_name)
    self._dump_run_params()

  def get_runner(self, task_name):
    """Get running instance of 'task_name'.

    Parameters
    ----------
    task_name: str
      Target task's name.

    Returns
    -------
    runner: TaskRunner, ProcRunner
      Reference runner of 'task_name'.

    """
    if task_name not in self._m_runner_dict:
      return None
    return self._m_runner_dict[task_name]["runner"]

  def _render_arguments(self, param):
    if isinstance(param, list):
      return list(map(self._render_arguments, param))

    if not isinstance(param, str):
      return param

    for match_str in re.findall(self._m_render_arg_pattern, param):
      match_str = match_str.strip()
      if match_str not in self._m_render_arguments:
        raise KeyError(
          "missing value for render argument '{0}'".format(match_str))

    def __lookup_func(reg_match):
      return self._m_render_arguments[reg_match.group(1).strip()]
    return self._m_render_arg_pattern.sub(__lookup_func, param)

  def _dump_run_params(self):
    if self._m_params is None or self._m_log_path is None:
      return

    save_path = os.path.join(self._m_log_path, "run_params")
    if os.path.exists(save_path):
      params_fname_pattern = re.compile("\d{8}_\d{6}")
      checkpoints = []
      for fname in os.listdir(save_path):
        if params_fname_pattern.match(fname):
          checkpoints.append(fname)
      checkpoints.sort(reverse=True)
      for fname in checkpoints[self._m_params_max_checkpoint_num:]:
        os.remove(fname)

    time_stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    save_fname = os.path.join(save_path, "%s.json" % (time_stamp))
    self._m_params.dump(save_fname)
