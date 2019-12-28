from lanfang.runner.base import RunnerStatus
from lanfang.runner.cmd_runner import CmdRunner
from lanfang.runner.func_runner import FuncProcessRunner
from lanfang.runner.multi_task_progress_ui import MultiTaskTableProgressUI
from lanfang.runner.multi_task_dependency import DynamicTopologicalGraph
from lanfang.runner.multi_task_context import RecordRunnerContext
from lanfang.runner.multi_task_context import DependentRunnerContext
from lanfang.utils import disk

import time
import signal
import os
import sys
import collections
import copy
import json
import logging
import datetime
import hashlib
import threading
import functools


class MultiTaskManager(object):
  pass


class Scheduler(object):
  pass


class ParallelScheduler(Scheduler):
  pass


class PipelineScheduler(Scheduler):
  pass


class PipelineRunner(object):
  pass


class TopologicalRunner(object):
  pass


class RunnerLogs(object):
  pass


class RunnerInventory(object):
  """Record a batch of tasks.
  """

  def __init__(self, *, context=None,
                        log_path=None,
                        log_mode='w+',
                        retry=1,
                        interval=5):
    self._m_context = context

    self._m_log_path = log_path
    if self._m_log_path is not None and not os.path.exists(self._m_log_path):
      os.makedirs(self._m_log_path)
    self._m_log_mode = log_mode
    self._m_log_files = []

    self._m_retry = retry
    self._m_interval = interval

    self._m_inventory = collections.OrderedDict()
    self._m_runners = {}
    self._m_lock = threading.Lock()
    self._m_closed = False
    self._m_restored_data = {}

  def __enter__(self):
    return self

  def __exit__(self, err_type, err_val, err_tb):
    self.close()

  def list(self):
    return list(self._m_inventory.keys())

  def close(self, force=False, timeout=None):
    self._m_lock.acquire()
    try:
      if self._m_closed is True:
        raise RuntimeError("RunnerInventory has been closed already.")

      for name in self._m_runners:
        if not self._m_runners[name]["runner"].is_alive():
          continue

        if not force:
          self._m_runners[name]["runner"].join(timeout=timeout)

        if self._m_runners[name]["runner"].is_alive():
          self._m_runners[name]["runner"].stop()
          self._m_runners[name]["status"] = RunnerStatus.KILLED

      for log_file in self._m_log_files:
        log_file.close()
        if os.path.getsize(log_file.name) == 0:
          os.remove(log_file.name)
      self._m_closed = True

    finally:
      self._m_lock.release()

  def save(self, checkpoint_path, max_checkpoint_num=5):
    self._m_lock.acquire()
    timestamp = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
    status = {}
    for task_name in self._m_runners:
      status[task_name] = self.get_info(task_name)
      status[task_name]["status"] = status[task_name]["status"].name
      status[task_name]["exitcode"] = \
          self._m_runners[task_name]["runner"].exitcode
    self._m_lock.release()

    status_file = os.path.join(
        checkpoint_path, "runner_status-{}.json".format(timestamp))
    with open(status_file, 'w') as fout:
      json.dump(status, fout, sort_keys=True, indent=2)

    disk.keep_files_with_pattern(
        path_dir=checkpoint_path,
        pattern="runner_status-\d{8}.\d{6}.json",
        max_keep_num=max_checkpoint_num,
        reverse=True)
    return status_file

  def restore(self, checkpoint_path):
    self._m_lock.acquire()
    try:
      with open(checkpoint_path, 'r') as fin:
        for name, task_status in json.load(fin).items():
          self._m_runners[name]["status"] = RunnerStatus[task_status["status"]]
          self._m_restored_data[name] = task_status
    except BaseException as e:
      logging.warning("Restore from checkpoint '%s' got exception '%s'",
          checkpoint_path, e)
    finally:
      self._m_lock.release()
    return self

  def new(self, context, log_path):
    """Create a new RunnerInventory with all the added tasks.

    Parameters
    ----------
    context: RunnerContext
      A context shared with multiple tasks.

    log_path: str
      The file path of the logs.

    Returns
    -------
    runner_inventory: RunnerInventory
      An RunnerInventory instance with
    """

    self._m_lock.acquire()
    inventory = self.__class__(
        context=context,
        log_path=log_path,
        log_mode=self._m_log_mode,
        retry=self._m_retry,
        interval=self._m_interval)

    for name, (target, runner_class, kwargs) in self._m_inventory.items():
      inventory.add(name, target, runner_class=runner_class, **kwargs)

    self._m_lock.release()
    return inventory

  def add(self, name, target, runner_class=None, **kwargs):
    """Add a new runner.

    Parameters
    ----------
    name: str
      The name of the task. Best using naming method of programming languages,
      for it may be used for creating log files on disk.

    target: list, str, callable object
      The target which needs to be executed.

    kwargs: dict
      Other arguments of runner.

    Returns
    -------
    instance: RunnerInventory
    """
    self._m_lock.acquire()
    try:
      if self._m_closed:
        raise RuntimeError(
            "Can't operate on a closed RunnerInventory instance.")

      if not isinstance(name, str):
        raise ValueError(
            "'name' of runner must be a str, but received '%s'" % name)

      if name in self._m_runners:
        raise KeyError("Runner '%s' is already exists." % name)

      self._m_inventory[name] = (target, runner_class, kwargs)

      if self._m_log_path is not None:
        for sname, suffix in [("stdout", "out"), ("stderr", "err")]:
          if sname in kwargs:
            logging.info("'%s' already been set for runner '%s'", sname, name)
            continue
          log_fname = "%s/%s.%s" % (self._m_log_path, name, suffix)
          kwargs[sname] = open(log_fname, self._m_log_mode)
          self._m_log_files.append(kwargs[sname])

      if "retry" not in kwargs:
        kwargs["retry"] = self._m_retry
      if "interval" not in kwargs:
        kwargs["interval"] = self._m_interval

      runner = self._get_runner_class(runner_class, target)(
          target, name=name, context=self._m_context, **kwargs)
      self._m_runners[runner.name] = {
        "status": RunnerStatus.WAITING,
        "runner": runner,
      }
    finally:
      self._m_lock.release()
    return self

  def status(self, name):
    return self._m_runners[name]["status"]

  def update_status(self, name, status):
    self._m_lock.acquire()
    try:
      if self._m_closed:
        raise RuntimeError(
            "Can't operate on a closed RunnerInventory instance.")
      self._m_runners[name]["status"] = status
    finally:
      self._m_lock.release()

  def start(self, name, recreate_if_necessary=False):
    self._m_lock.acquire()
    try:
      if self._m_closed:
        raise RuntimeError(
            "Can't operate on a closed RunnerInventory instance.")
      self._m_restored_data.pop(name, None)

      if recreate_if_necessary and \
              self._m_runners[name]["runner"].ident is not None:
        if self._m_runners[name]["runner"].is_alive():
          raise RuntimeError("Can't recreate a new runner "
              "since previous runner is alive for '%s'" % (name))
        else:
          target, runner_class, kwargs = self._m_inventory[name]
          runner_class = self._get_runner_class(runner_class, target)
          self._m_runners[name]["runner"] = runner_class(
              target, name=name, context=self._m_context, **kwargs)

      self._m_runners[name]["status"] = RunnerStatus.RUNNING
      self._m_runners[name]["runner"].start()
    finally:
      self._m_lock.release()

  def is_alive(self, name):
    if name in self._m_restored_data:
      return False
    return self._m_runners[name]["runner"].is_alive()

  def exitcode(self, name):
    if name in self._m_restored_data:
      return self._m_restored_data[name]["exitcode"]
    return self._m_runners[name]["runner"].exitcode

  def get_info(self, name):
    status = self._m_runners[name]["status"]
    if name not in self._m_restored_data:
      runner = self._m_runners[name]["runner"]
      start_time = runner.start_time
      elapsed_time = runner.elapsed_time
      attempts = runner.attempts
    else:
      start_time = self._m_restored_data[name]["start_time"]
      elapsed_time = self._m_restored_data[name]["elapsed_time"]
      attempts = self._m_restored_data[name]["attempts"]

    return {
      "status": status,
      "start_time": start_time,
      "elapsed_time": elapsed_time,
      "attempts": attempts
    }

  def _get_runner_class(self, runner_class, target):
    if runner_class is not None:
      return runner_class

    if callable(target):
      return FuncProcessRunner
    else:
      return CmdRunner


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

  config_file: str
    Configure file for these tasks.

  config_kwargs: dict
    Parameters to initialize configuration instance.

  max_checkpoint_num: int
    The maximum number of params checkpoints to save.

  runner_progresss_ui_class: MultiTaskProgressUI
    Class to dynamically display tasks running status.
  """

  def __init__(self, *, log_path=None,
                        parallel_degree=-1,
                        retry=1,
                        interval=5,
                        config_file=None,
                        config_kwargs={},
                        params=None,
                        runner_progresss_ui_class=MultiTaskTableProgressUI):
    self._m_log_path = log_path
    self._m_parallel_degree = parallel_degree
    self._m_config_file = config_file
    self._m_config_kwargs = config_kwargs
    self._m_params = params
    self._m_runner_progress_ui_class = runner_progresss_ui_class

    self._m_pid = os.getpid()
    self._m_lock = threading.Lock()
    self._m_runner_code_snippet = []
    self._m_runner_inventory = RunnerInventory(retry=retry, interval=interval)
    self._m_cached_running_history = collections.OrderedDict()
    self._m_runner_dependency = DynamicTopologicalGraph()

  def __enter__(self):
    return self

  def __exit__(self, err_type, err_val, err_tb):
    self.close()

  def set_params(self, params):
    self._m_params = params

  def get_params(self):
    return copy.deepcopy(self._m_params)

  def close(self, force=False, timeout=None):
    """Close all key resources.
    """
    self._m_runner_inventory.close()
    for key, record in self._m_cached_running_history.items():
      record["runner_inventory"].close(force=force, timeout=timeout)
    self._m_cached_running_history.clear()

  def save(self, checkpoint_path, *, params=None, max_checkpoint_num=5):
    """Save the status into disk.

    Parameters
    ----------
    checkpoint_path: str
      The directory to save checkpoint files.

    params: dict
      The parameters of the running session.

    max_checkpoint_num: int
      Maximum number of checkpoints.

    Returns
    -------
    checkpoint_info: dict
      The information of the checkpoint.
    """

    if checkpoint_path is None:
      return

    if not os.path.exists(checkpoint_path):
      os.makedirs(checkpoint_path)

    if not os.path.isdir(checkpoint_path):
      raise IOError("Target checkpoint path '%s' is not a directory." % (
          checkpoint_path))

    if params is None:
      params = self._m_params
    record = self._get_cached_history(params)

    runner_inventory_file = record["runner_inventory"].save(
            checkpoint_path, max_checkpoint_num=max_checkpoint_num)

    context_file = record["context"].save(
            checkpoint_path, max_checkpoint_num=max_checkpoint_num)

    all_checkpoints_file = os.path.join(checkpoint_path, "all_checkpoints.json")
    checkpoints_record = {}
    if os.path.isfile(all_checkpoints_file):
      with open(all_checkpoints_file, 'r') as fin:
        try:
          checkpoints_record = json.load(fin)
        except:
          pass

    checkpoints_record.pop("checkpoint", None)
    timestamp = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
    checkpoints_record[timestamp] = {
      "runner_inventory": runner_inventory_file,
      "context": context_file,
      "params": params
    }

    if max_checkpoint_num <= 0:
      max_checkpoint_num = len(checkpoints_record)

    remove_keys = sorted(checkpoints_record, reverse=True)[max_checkpoint_num:]
    for checkpoint_index in remove_keys:
      checkpoints_record.pop(checkpoint_index)
    checkpoints_record["checkpoint"] = timestamp

    with open(all_checkpoints_file, 'w') as fout:
      json.dump(checkpoints_record, fout, indent=2, sort_keys=True)

    return checkpoints_record[timestamp]

  def restore(self, checkpoint_path):
    if os.path.isfile(checkpoint_path):
      all_checkpoints_file = checkpoint_path
    else:
      all_checkpoints_file = os.path.join(
          checkpoint_path, "all_checkpoints.json")

    with open(all_checkpoints_file, 'r') as fin:
      checkpoints_record = json.load(fin)
      restore_info = checkpoints_record[checkpoints_record["checkpoint"]]

      self.set_params(restore_info["params"])
      cached_record = self._get_cached_history(restore_info["params"])

      cached_record["context"].restore(restore_info["context"])
      cached_record["runner_inventory"].restore(
          restore_info["runner_inventory"])

    return self

  def add(self, name, target, *, depends=None, **kwargs):
    """Add a new runner.

    Parameters
    ----------
    name: str
      The name of the task. Best using naming method of programming languages,
      for it may be used for creating log files on disk.

    target: list, str, callable object
      The target which needs to be executed.

    depends: str, list, set, dict
      List of depended runners.
      If this is a string, multiple runners can be separated by a single comma.

    Returns
    -------
    self: MultiTaskRunner
      Reference for current instance.
    """

    self._m_runner_inventory.add(name, target, **kwargs)
    self._m_runner_dependency.add(name, depends)
    for key, record in self._m_cached_running_history.items():
      record["runner_inventory"].add(name, target, **kwargs)
    return self

  def adds(self, runner_str):
    """Add runner from string.

    Parameters
    ----------
    runner_str: str
      The string of the runner, which is a python executable code.

    Returns
    -------
    self: RunnerInventory
      Reference for current instance.
    """

    exec(runner_str, {}, {'Runner': self.add})
    return self

  def addf(self, runner_fname, encoding="utf-8"):
    """Add tasks from a file.

    Parameters
    ----------
    runner_fname: str
      The file which contains the runner code snippet.

    encoding: str
      The encode of file content specified by `runner_fname'.

    Returns
    -------
    self: RunnerInventory
      Reference for current instance.
    """

    with open(runner_fname, mode='r', encoding=encoding) as fin:
      return self.adds(fin.read())

  def add_dependency(self, task_name, depends):
    """Add dependent relations for a task.

    Parameters
    ----------
    task_name: str
      The name of the task need to add dependency.

    depends: string, list, tuple, set, dict
      List of depended tasks.
    """
    self._m_dependency.add(task_name, depends)

  def list(self, *, params=None, verbose=False):
    """List all tasks.

    Parameters
    ----------
    verbose: boolean
      Set True if you want to display the tasks info on the screen.

    Returns
    -------
    tasks: list
      The list of tasks in order of running id.

    Raises
    ------
    ValueError: If the tasks relations is not topological.
    """

    if not self._m_runner_dependency.is_valid():
      raise ValueError("The dependency relations of tasks is not topological")

    if verbose is True:
      if params is None:
        params = self._m_params

      if params is not None:
        runner_inventory = self._get_cached_history(params)["runner_inventory"]
      else:
        runner_inventory = self._m_runner_inventory

      progress_ui = self._m_runner_progress_ui_class(
          runner_inventory, self._m_runner_dependency)
      progress_ui.display(reuse=False)
    return self._m_runner_dependency.get_nodes(order=True)

  def run(self, tasks=None, *, params=None, verbose=False, try_best=False):
    """Run a bunch of tasks.

    Parameters
    ----------
    tasks: str
      The tasks which needed to be executed,
      see more details of 'DependencyManager.subset'.

    params: dict
      The parameters needed to run the tasks.

    verbose: boolean
      Print verbose information.

    try_best: boolean
      Set true if you want to executed the tasks as many as possible
      even if there exist some failed tasks.

    Returns
    -------
    result : integer
      Number of failed tasks.

    Raises
    ------
    RuntimeError:
      If the set of tasks is not topological or has been exected already.
    """

    if not self._m_runner_dependency.is_valid():
      raise RuntimeError("Dependent relations of tasks is not topological")

    self._m_lock.acquire()
    try:
      handler = functools.partial(self._kill_signal_handler, run_params=params)
      prev_sigint_handler = signal.signal(signal.SIGINT, handler)
      prev_sigterm_handler = signal.signal(signal.SIGTERM, handler)
      return self._run_multiple_tasks(
          tasks=tasks, params=params, verbose=verbose, try_best=try_best)
    finally:
      signal.signal(signal.SIGINT, prev_sigint_handler)
      signal.signal(signal.SIGTERM, prev_sigterm_handler)
      self._m_lock.release()

  def _run_multiple_tasks(self, tasks=None, *,
                                params=None,
                                verbose=False,
                                try_best=False):
    if params is None:
      params = self._m_params
    history_info = self._get_cached_history(params)
    runner_inventory = history_info["runner_inventory"]
    context = history_info["context"]

    dependency = self._m_runner_dependency.subset(tasks)
    enabled_tasks = set(dependency.get_nodes())
    if verbose:
      progress_ui = self._m_runner_progress_ui_class(
          runner_inventory, dependency)
      progress_ui.display()
    else:
      progress_ui = None

    previous_input = {}
    for task_name in enabled_tasks:
      previous_input[task_name] = context.get_input(task_name)

    remaining_tasks = set(enabled_tasks)
    running_tasks = set()
    succeed_tasks = set()
    failed_tasks = set()
    while True:
      for task_name in dependency.top():
        if task_name not in remaining_tasks:
          continue

        # task succeed already.
        if runner_inventory.status(task_name) == RunnerStatus.DONE \
              and context.get_input(task_name) == previous_input[task_name]:
          remaining_tasks.remove(task_name)
          running_tasks.add(task_name)
          continue

        if self._m_parallel_degree < 0 or len(
                running_tasks) < self._m_parallel_degree:
          remaining_tasks.remove(task_name)
          running_tasks.add(task_name)
          runner_inventory.start(task_name, recreate_if_necessary=True)
        else:
          runner_inventory.update_status(task_name, RunnerStatus.READY)

      for task_name in running_tasks.copy():
        if runner_inventory.is_alive(task_name):
          continue
        running_tasks.remove(task_name)

        exitcode = runner_inventory.exitcode(task_name)
        if exitcode != 0 and exitcode is not None:
          runner_inventory.update_status(task_name, RunnerStatus.FAILED)
          logging.critical(
              "Task %s failed, exit with code '%s'", task_name, exitcode)
          failed_tasks.add(task_name)
          offspring = dependency.reverse_depends(task_name, recursive=True)
          failed_tasks |= offspring
          for name in offspring:
            runner_inventory.update_status(name, RunnerStatus.CANCELED)
        else:
          runner_inventory.update_status(task_name, RunnerStatus.DONE)
          succeed_tasks.add(task_name)
          dependency.remove(task_name)

      verbose and progress_ui.display()
      if len(succeed_tasks) + len(failed_tasks) == len(enabled_tasks):
        break
      if not try_best and len(failed_tasks) > 0:
        self.stop()
        break
      time.sleep(0.1)

    if verbose:
      progress_ui.display(reuse=False)
      progress_ui.clear()
    return len(failed_tasks)

  def _get_cached_history(self, params):
    params_str = json.dumps(params, sort_keys=True).encode("utf-8")
    params_hashkey = hashlib.md5(params_str).hexdigest()
    if params_hashkey in self._m_cached_running_history:
      return self._m_cached_running_history[params_hashkey]

    if self._m_config_file is not None:
      context = DependentRunnerContext(
          task_config_file=self._m_config_file, **self._m_config_kwargs)
    else:
      context = RecordRunnerContext()
    context.set_params(params)

    if len(self._m_cached_running_history) == 0:
      log_path = self._m_log_path
    else:
      log_path = os.path.join(self._m_log_path, params_hashkey)

    self._m_cached_running_history[params_hashkey] = {
      "params": copy.deepcopy(params),
      "runner_inventory": self._m_runner_inventory.new(
          context=context, log_path=log_path),
      "context": context
    }
    return self._m_cached_running_history[params_hashkey]

  def _kill_signal_handler(self, signum, stack, run_params=None):
    # Every subproces will receive the same signal,
    # we process the signal in the same process with MultiTaskRunner.
    if os.getpid() != self._m_pid:
      return
    logging.warning("Receive signal %s, try to kill all running runners.",
        signal.Signals(signum).name)
    self.stop()
    self.list(params=run_params, verbose=True)
    logging.warning("All runners are killed.")
    sys.exit(1)

  def stop(self):
    """Stop all runners."""
    for task_name, record in self._m_cached_running_history.items():
      record["runner_inventory"].close(force=True)
