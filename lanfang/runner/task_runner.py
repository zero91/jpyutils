import sys
import os
import subprocess
import threading
import time
import signal
import logging
import json
import copy
import shlex
import multiprocessing
from multiprocessing import managers


class TaskRunner(threading.Thread):
  """Execute a command in an independent process and
  maintain the new created process in a thread.

  The whole class signature is largely the same as of Popen,
  all other arguments are passed directly through to the Popen.

  Parameters
  ----------
  target: list, str
    The command to be executed.

    It should be a sequence of program arguments or a single string.
    By default, the program to be executed is the first item
    if 'target' is a sequence.

    The shell argument (which defaults to False) specifies whether
    to use the shell as the program to execute.

    If shell is True, it is recommended to pass 'target' as a string
    rather than a sequence.

  name: str
    The name of the task. Best use naming method of programming languages.

  retry: integer
    Try executing the command 'retry' times until succeed, otherwise failed.

  interval: float
    Interval time between each try.

  daemon: boolean
    A boolean value indicating whether this runner
    is a daemon process (True) or not (False).

  pre_hook: callable
    A callable object to be invoked before the command starts.
    Unlike ProcRunner, the input parameters is (command, config_params).
    The first argument is the command in a list of strings
    which is used by execute.
    The second argument is the configuration values.

  post_hook: callable
    A callable object to be invoked after the command execution.
    The input parameters is the return value of the command.

  share_dict: multiprocessing.managers.DictProxy
    A multiple process sharing dictionary which can be used to
    exchange data between processes.

    Can be created use the code:

      with Manager() as manager:
        share_dict = manager.dict()

  encoding: str
    The encoding of the data output by command's stdout.
    The stdout outputs of the command will be treated as the command
    return value, and is parsed as an json dump string using ecoding
    as 'encoding' specifies.

  popen_kwargs: dict
    Arguments which is supported by subprocess.Popen.

  Properties
  ----------
  daemon: Whether this process is daemon or not.

  name: The name of this process.

  stdin: stdin attribute of the running process.

  stdout: stdout attribute of the running process.

  stderr: stderr attribute of the running process.

  exitcode: The exit code after execute target.

  Notes
  -----
  Child process can access configuration parameters through
  the environment variable 'TASK_RUNNER_PARAMETERS',
  which is a json string.

  """
  def __init__(self, target, name=None, retry=1, interval=5, daemon=True,
                             pre_hook=None, post_hook=None,
                             share_dict=None, share_dict_lock=None,
                             encoding="utf-8", **popen_kwargs):
    if not isinstance(target, (str, list, tuple)):
      raise TypeError("Parameter 'target' should be a string or a list")

    if share_dict is not None and \
        not isinstance(share_dict, multiprocessing.managers.DictProxy):
      raise TypeError("Parameter 'share_dict' should be type " \
          "multiprocessing.managers.DictProxy")

    super(__class__, self).__init__(target=target, name=name)
    self.daemon = daemon
    self.exitcode = None

    self.stdin = popen_kwargs.get("stdin", None)
    self.stdout = popen_kwargs.get("stdout", None)
    self.stderr = popen_kwargs.get("stderr", None)

    self._m_retry_limit = retry
    self._m_retry_interval = interval

    self._m_pre_hook = pre_hook
    self._m_post_hook = post_hook

    self._m_share_dict = share_dict
    self._m_share_dict_lock = share_dict_lock
    self._m_encoding = encoding
    self._m_popen_kwargs = popen_kwargs

    self._m_lock = threading.Lock()
    self._m_run_process = None
    self._m_stop_flag = threading.Event()

    self._m_return_value = None
    self._m_try_num = 0
    self._m_start_time = None
    self._m_elapsed_time = None

  def __del__(self):
    if self._m_run_process is not None:
      if self._m_run_process.stdin is not None:
        self._m_run_process.stdin.close()

      if self._m_run_process.stdout is not None:
        self._m_run_process.stdout.close()

      if self._m_run_process.stderr is not None:
        self._m_run_process.stderr.close()

  def run(self):
    self.__update_start_time()

    # Reset Parameters of Popen
    #if "preexec_fn" in self._m_popen_kwargs:
    #  raise ValueError("Argument 'preexec_fn' is not allowed, "\
    #      "it will be used internally.")
    #self._m_popen_kwargs['preexec_fn'] = os.setsid
    self._m_popen_kwargs['start_new_session'] = True

    if "close_fds" not in self._m_popen_kwargs:
      self._m_popen_kwargs["close_fds"] = True

    # Reset stdout temporarily to get process return value.
    if self._m_popen_kwargs.get("stdout") in [
            subprocess.PIPE, subprocess.DEVNULL]:
      stdout_stream = None
    else:
      stdout_stream = self._m_popen_kwargs.get("stdout", sys.stdout)
    self._m_popen_kwargs["stdout"] = subprocess.PIPE

    if "env" not in self._m_popen_kwargs:
      self._m_popen_kwargs["env"] = copy.deepcopy(os.environ)

    # Setup Shared Parameters
    input_params = self.__get_shared_params()
    self._m_popen_kwargs["env"]["TASK_RUNNER_PARAMETERS"] = json.dumps(
      input_params)

    # Execute Pre-Hook
    self.__execute_pre_hook(input_params)

    target_ret_value = None
    last_exitcode = None
    while not self._m_stop_flag.is_set() \
        and self._m_try_num < self._m_retry_limit:

      if self._m_try_num > 0:
        logging.info("Wait %.2f seconds", self._m_retry_interval)
        time.sleep(self._m_retry_interval)
      self._m_try_num += 1

      self._m_run_process = subprocess.Popen(
        self._target, **self._m_popen_kwargs)
      while self._m_run_process.stdout is None:
        time.sleep(0.1)

      self.stdin = self._m_run_process.stdin
      self.stderr = stdout_stream
      self.stdout = self._m_run_process.stdout

      stdout_lines = list()
      while True:
        line = self._m_run_process.stdout.readline()
        if len(line) == 0 and self._m_run_process.poll() is not None:
          break

        if len(line) > 0:
          line = line.decode(self._m_encoding)
          stdout_lines.append(line)
          if stdout_stream is not None:
            stdout_stream.write(line)
            stdout_stream.flush()

      last_exitcode = self._m_run_process.poll()
      if last_exitcode != 0:
        logging.warning("Proc '%s' exit with non-zero code '%d' " \
          "on try %d/%d" % (self.name, last_exitcode, self._m_try_num,
                            self._m_retry_limit))
      else:
        target_ret_value = self.__decode_stdout_value(stdout_lines)
        break

    if last_exitcode != 0:
      self.exitcode = last_exitcode
      self._m_elapsed_time = time.time() - self._m_start_time
      exit(last_exitcode)

    # Execute Post-Hook
    self.__execute_post_hook(target_ret_value)

    # Update Shared Parameters
    self.__set_shared_params(target_ret_value)

    self._m_return_value = copy.deepcopy(target_ret_value)
    self._m_elapsed_time = time.time() - self._m_start_time
    self.exitcode = last_exitcode
    return self.exitcode

  def __update_start_time(self):
    self._m_lock.acquire()
    if self._m_start_time is not None:
      raise RuntimeError("cannot start a process twice")
    self._m_start_time = time.time()
    self._m_lock.release()

  def __get_shared_params(self):
    input_params = {}
    if self._m_share_dict is not None \
        and self.name in self._m_share_dict \
        and "input" in self._m_share_dict[self.name]:
      if self._m_share_dict_lock is not None:
        self._m_share_dict_lock.acquire()
      try:
        input_params = self._m_share_dict[self.name]["input"]
      finally:
        if self._m_share_dict_lock is not None:
          self._m_share_dict_lock.release()
    return input_params

  def __set_shared_params(self, return_value):
    if self._m_share_dict is None:
      return

    if self._m_share_dict_lock is not None:
      self._m_share_dict_lock.acquire()

    try:
      params = self._m_share_dict.get(self.name, dict())
      params.update({"output": return_value})
      self._m_share_dict.update({self.name: params})
    finally:
      if self._m_share_dict_lock is not None:
        self._m_share_dict_lock.release()

  def __execute_pre_hook(self, input_params):
    if isinstance(self._target, str):
      target = shlex.split(self._target)
    else:
      target = copy.deepcopy(self._target)

    if self._m_pre_hook is not None \
        and self._m_pre_hook(target, input_params) not in (0, None):
      self._m_elapsed_time = time.time() - self._m_start_time
      raise RuntimeError(
        "Execute pre_hook failed, please check the input parameters")

  def __execute_post_hook(self, return_value):
    if self._m_post_hook is not None and \
        self._m_post_hook(copy.deepcopy(return_value)) not in (0, None):
      self._m_elapsed_time = time.time() - self._m_start_time
      raise RuntimeError(
        "Execute post_hook failed, please check the output values")

  def __decode_stdout_value(self, stdout_lines):
    stdout_data = "".join(stdout_lines).strip()
    for data in [stdout_data, stdout_data.split('\n')[-1]]:
      try:
        return json.loads(data)
      except TypeError as te:
        return json.loads(data.decode(self._m_encoding))
      except BaseException as be:
        pass
    return {}

  def terminate(self):
    """Terminate current running process."""
    self._m_stop_flag.set()
    if self._m_run_process is not None and self._m_run_process.poll() is None:
      os.killpg(self._m_run_process.pid, signal.SIGTERM)

  @property
  def info(self):
    """Get task's internal information.

    Returns
    -------
    information: dict
      A dictionnary which contains information of task, including:
        {
          start_time: started time of current task in seconds,
          elapsed_time: time since task started in seconds,
          exitcode: exit code of the task,
          return: return value of the task extracted from stdout output,
          try: try information of the task,
        }
    """
    if self.is_alive() and self._m_start_time is not None:
      self._m_elapsed_time = time.time() - self._m_start_time

    return {
      "start_time": self._m_start_time,
      "elapsed_time": self._m_elapsed_time,
      "exitcode": self.exitcode,
      "return": self._m_return_value,
      "try": "%s/%s" % (self._m_try_num, self._m_retry_limit)
    }
