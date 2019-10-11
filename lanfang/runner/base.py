import copy
import enum
import collections
import time
import json
import hashlib
import threading
import abc
import multiprocessing
import logging


class RunnerStatus(enum.Enum):
  DISABLED = 0
  WAITING  = 1
  READY    = 2
  RUNNING  = 3
  DONE     = 4
  FAILED   = 5
  KILLED   = 6
  CANCELED = 7


class SharedScope(enum.Enum):
  THREAD = 1
  PROCESS = 2


class SharedData(collections.MutableMapping):
  """Shared data between multiple processes or threads.

  Parameters
  ----------
  shared_scope: SharedScope
    The data sharing scope, can be SharedScope.THREAD or SharedScope.PROCESS.

  """
  def __init__(self, *args, shared_scope=SharedScope.THREAD, **kwargs):
    self._m_shared_scope = shared_scope

    if self._m_shared_scope == SharedScope.THREAD:
      self._m_shared_dict = dict(*args, **kwargs)
      self._m_lock = threading.Lock()

    elif self._m_shared_scope == SharedScope.PROCESS:
      self._m_manager = multiprocessing.Manager()
      self._m_shared_dict = self._m_manager.dict(*args, **kwargs)
      self._m_lock = self._m_manager.Lock()
      #self._m_lock = multiprocessing.Lock()
    else:
      raise ValueError("Unsupported 'shared_scope': {}".format(shared_scope))

    # Data used when the connection is broken.
    self._m_standby_shared_data = self._m_shared_dict.copy()

  def _acquire(self):
    try:
      self._m_lock.acquire()
    except (ConnectionError, EOFError) as e:
      logging.warning("%s._acquire got exception %s: %s",
          self.__class__.__name__, type(e), e)

  def _release(self):
    try:
      self._m_lock.release()
    except (ConnectionError, EOFError) as e:
      logging.warning("%s._release got exception %s: %s",
          self.__class__.__name__, type(e), e)

  def update(self, *args, **kwds):
    try:
      self._acquire()
      self._m_shared_dict.update(*args, **kwds)
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      self._m_standby_shared_data.update(*args, **kwds)
      logging.warning("%s.update got exception %s: %s",
          self.__class__.__name__, type(e), e)
    finally:
      self._release()

  def __getitem__(self, key):
    try:
      self._acquire()
      if self._m_shared_scope == SharedScope.THREAD:
        # To ensure the behaviors for threads and processes are the same,
        # we deep copy the value data in multiple threads environments.
        value = copy.deepcopy(self._m_shared_dict[key])
      else:
        value = self._m_shared_dict[key]
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__getitem__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      value = copy.deepcopy(self._m_standby_shared_data[key])
    finally:
      self._release()

    return value

  def __setitem__(self, key, value):
    try:
      self._acquire()
      self._m_shared_dict[key] = value
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__setitem__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      self._m_standby_shared_data[key] = value
    finally:
      self._release()

  def __delitem__(self, key):
    try:
      self._acquire()
      del self._m_shared_dict[key]
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as be:
      logging.warning("%s.__delitem__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      del self._m_standby_shared_data[key]
    finally:
      self._release()

  def __iter__(self):
    try:
      self._acquire()
      keys = self._m_shared_dict.keys()
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__iter__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      keys = self._m_standby_shared_data.keys()
    finally:
      self._release()

    return iter(keys)

  def __len__(self):
    try:
      self._acquire()
      length = len(self._m_shared_dict)
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__len__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      length = len(self._m_standby_shared_data)
    finally:
      self._release()

    return length

  def __str__(self):
    try:
      self._acquire()
      self._m_standby_shared_data = self._m_shared_dict.copy()
      return str(self._m_standby_shared_data)

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__str__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      return str(self._m_standby_shared_data)
    finally:
      self._release()

  def __hash__(self):
    try:
      self._acquire()
      frozen_value = json.dumps(self._m_shared_dict.copy(), sort_keys=True)
      self._m_standby_shared_data = self._m_shared_dict.copy()

    except (ConnectionError, EOFError) as e:
      logging.warning("%s.__hash__ got exception %s: %s",
          self.__class__.__name__, type(e), e)
      frozen_value = json.dumps(self._m_standby_shared_data, sort_keys=True)
    finally:
      self._release()
    return int(hashlib.md5(frozen_value.encode("utf-8")).hexdigest(), 16)


class Runner(abc.ABC):
  """Base abstract class for running a target.

  Parameters
  ----------
  target: object
    The target to execute.

  name: str
    The name of the task. Best using naming method of programming languages.

  retry: integer
    Try executing the target 'retry' times until succeed, otherwise failed.

  interval: float
    Interval time between each try.

  daemon: boolean
    A boolean value indicating whether this runner
    is a daemon process/thread (True) or not (False).

  pre_hooks: list of callable objects
    A list of callable objects to be invoked before the target starts.
    Each callable objects should have signature '(target, input_params)'.

  post_hooks: list of callable objects
    A list of callable objects to be invoked after the target execution.
    The input parameters is the return value of the command.
    Each callable objects should have signature '(output_values)'.

  stdin: stream
    Input stream.

  stdout: stream
    Output stream.

  stderr: stream
    Error output stream.

  shared_data: SharedData
    The input/output parameters to be shared
    between multiple processes/threads.

  internal_scope: SharedScope
    The internal data sharing scope of this runner.

  Properties
  ----------
  name: The name of this runner.

  start_time: The start time of this runner.

  elapsed_time: The time elapsed time since the runner begins.

  output: The output value of this runner.

  attempts: tuple (attempts, retry), The attempts info of this runner.

  exitcode: The exit code after execute target.

  stdin: stdin stream.

  stdout: stdout stream.

  stderr: stderr stream.

  """
  def __init__(self, target, *, name=None, retry=1, interval=5, daemon=None,
                             pre_hooks=None, post_hooks=None,
                             stdin=None, stdout=None, stderr=None,
                             shared_data=None,
                             internal_scope=SharedScope.THREAD):
    self._m_target = target
    self._m_name = name
    self._m_retry_limit = retry
    self._m_retry_interval = interval
    self._m_daemon = daemon

    if pre_hooks is not None and not isinstance(pre_hooks, (list, tuple)):
      raise TypeError("Parameter 'pre_hooks' must be a list.")
    self._m_pre_hooks = pre_hooks if pre_hooks is not None else []

    if post_hooks is not None and not isinstance(post_hooks, (list, tuple)):
      raise TypeError("Parameter 'post_hooks' must be a list.")
    self._m_post_hooks = post_hooks if post_hooks is not None else []

    self.stdin = stdin
    self.stdout = stdout
    self.stderr = stderr

    self._m_shared_data = shared_data

    status = {
      "attempts": 0,
      "start_time": None,
      "elapsed_time": None,
      "exitcode": None,
    }
    self._m_runner_status = SharedData(status, shared_scope=internal_scope)

  @property
  def name(self):
    return self._m_name

  @property
  def start_time(self):
    return self._m_runner_status["start_time"]

  @property
  def elapsed_time(self):
    start_time = self.start_time
    if start_time is not None and self.is_alive():
      return time.time() - start_time
    return self._m_runner_status["elapsed_time"]

  @property
  def output(self):
    return self._m_runner_status.get("output")

  @property
  def attempts(self):
    return self._m_runner_status["attempts"], self._m_retry_limit

  @property
  def exitcode(self):
    return self._m_runner_status["exitcode"]

  @abc.abstractmethod
  def is_alive(self):
    """Return whether this runner is alive."""
    pass

  @abc.abstractmethod
  def _execute_target(self, input_params):
    """Execute the target.

    Returns
    -------
    exitcode: int
      The exit code of the this execution.

    return_value: dict
      The return value of the target.
    """
    pass

  def run(self):
    if self._m_runner_status["start_time"] is not None:
      raise RuntimeError("runner can only be started once")
    self._m_runner_status["start_time"] = time.time()

    shared_input_params = self._fetch_shared_input_params()
    input_params = self._fetch_input_params(shared_input_params)

    # Run pre hooks
    self._execute_pre_hooks(input_params)

    # Run target
    while not self.stopped() and \
            self._m_runner_status["attempts"] < self._m_retry_limit:
      if self._m_runner_status["attempts"] > 0:
        logging.info("Wait %.2f seconds", self._m_retry_interval)
        time.sleep(self._m_retry_interval)
      self._m_runner_status["attempts"] += 1

      exitcode, output_values = self._execute_target(input_params)
      if exitcode == 0:
        self._m_runner_status["exitcode"] = exitcode
        break
      elif self._m_runner_status["attempts"] >= self._m_retry_limit:
        self._m_runner_status["elapsed_time"] = \
            time.time() - self._m_runner_status["start_time"]
        self._m_runner_status["exitcode"] = exitcode
        exit(exitcode)

    # Run post hooks
    self._execute_post_hooks(output_values)

    self._set_shared_output_params(output_values)
    self._m_runner_status["output"] = output_values
    self._m_runner_status["elapsed_time"] = \
        time.time() - self._m_runner_status["start_time"]

  def _fetch_shared_input_params(self):
    try:
      if self._m_shared_data is not None:
        return self._m_shared_data[self._m_name]["input"]
      else:
        return {}
    except KeyError as ke:
      return {}

  @abc.abstractmethod
  def _fetch_input_params(self, shared_input_params):
    """Extract all input parameters."""
    pass

  def _set_shared_output_params(self, output_values):
    if self._m_shared_data is None:
      return
    params = self._m_shared_data.get(self._m_name, {})
    params.update({"output": output_values})
    self._m_shared_data.update({self._m_name: params})

  def _execute_pre_hooks(self, input_params):
    for hook in self._m_pre_hooks:
      if hook is None:
        continue

      try:
        ret = hook(self._m_target, copy.deepcopy(input_params))
      except BaseException as e:
        self._m_runner_status["elapsed_time"] = \
            time.time() - self._m_runner_status["start_time"]
        raise e

      if ret not in (0, None):
        self._m_runner_status["elapsed_time"] = \
            time.time() - self._m_runner_status["start_time"]
        raise RuntimeError("Execute pre_hook '%s' failed with '%s', "
            "please check the input parameters" % (hook, ret))

  def _execute_post_hooks(self, output_values):
    for hook in self._m_post_hooks:
      if hook is None:
        continue
      ret = hook(copy.deepcopy(output_values))
      if ret not in (0, None):
        self._m_runner_status["elapsed_time"] = \
            time.time() - self._m_runner_status["start_time"]
        raise RuntimeError("Execute post_hook '%s' failed with '%s', "
            "please check the output values" % (hook, ret))

  @abc.abstractmethod
  def stop(self):
    """Stop this runner."""
    pass

  @abc.abstractmethod
  def stopped(self):
    """Return whether this runner has been stopped."""
    pass
