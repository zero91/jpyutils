from lanfang.runner.base import Runner, SharedScope

import sys
import os
import subprocess
import threading
import time
import signal
import json
import copy
import argparse


__TASK_ENV_PARAMS__ = 'TASK_RUNNER_PARAMETERS'


class CmdRunner(Runner, threading.Thread):
  """Execute a command in an independent process
  and maintain the new created process in a thread.

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

  encoding: str
    The encoding of the stdout data output by this runner.
    The stdout outputs of the runner will be
    treated as the runner's return value,
    and is parsed as an json dump string using ecoding
    as 'encoding' specifies.

  popen_kwargs: dict
    Arguments which is supported by subprocess.Popen.

  Properties
  ----------
  daemon: Whether this process/thread is daemon or not.

  Notes
  -----
  Child process can access configuration parameters through
  the environment variable 'TASK_RUNNER_PARAMETERS',
  which is a json string.
  """

  __doc__ += "\nDocument of Runner\n" + ("-" * 20) + "\n" + Runner.__doc__

  def __init__(self, target, *, name=None, retry=1, interval=5, daemon=None,
                             hooks=None, context=None,
                             stdin=None, stdout=None, stderr=None,
                             encoding="utf-8", **popen_kwargs):
    if not isinstance(target, (str, list, tuple)):
      raise TypeError("Parameter 'target' should be a string or a list.")

    threading.Thread.__init__(self, target=target, name=name, daemon=daemon)

    Runner.__init__(
      self, target=target, name=name, retry=retry, interval=interval,
      daemon=daemon, hooks=hooks, context=context,
      stdin=stdin, stdout=stdout, stderr=stderr,
      internal_scope=SharedScope.THREAD)

    self._m_name = self.name
    self._m_encoding = encoding
    self._m_popen_kwargs = popen_kwargs
    self._m_popen_kwargs.update(
        {"stdin": stdin, "stdout": stdout, "stderr": stderr})

    self._m_stop_event = threading.Event()
    self._m_run_process = None

  def _execute_target(self, input_params):
    popen_kwargs = self._m_popen_kwargs
    popen_kwargs['start_new_session'] = True
    if "close_fds" not in popen_kwargs:
      popen_kwargs["close_fds"] = True

    # Reset stdout temporarily to get process return value.
    if popen_kwargs.get("stdout") in [subprocess.PIPE, subprocess.DEVNULL]:
      stdout_stream = None
    elif popen_kwargs.get("stdout") is None:
      stdout_stream = sys.stdout
    else:
      stdout_stream = popen_kwargs.get("stdout")
    backup_stdout = popen_kwargs.get("stdout")
    popen_kwargs["stdout"] = subprocess.PIPE

    if "env" not in popen_kwargs:
      popen_kwargs["env"] = copy.deepcopy(os.environ)

    # Setup Shared Parameters
    popen_kwargs["env"][__TASK_ENV_PARAMS__] = json.dumps(input_params)

    self._m_run_process = subprocess.Popen(self._m_target, **popen_kwargs)
    while self._m_run_process.stdout is None:
      time.sleep(0.1)

    stdout_lines = []
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

    popen_kwargs["stdout"] = backup_stdout
    exitcode = self._m_run_process.poll()

    for stream in [self._m_run_process.stdin,
                   self._m_run_process.stdout,
                   self._m_run_process.stderr]:
      if stream is not None:
        stream.close()

    if exitcode != 0:
      return exitcode, None
    else:
      ret_value = self.__decode_stdout_value(stdout_lines)
      return 0, ret_value

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

  def _fetch_input_params(self, params):
    return params

  def is_alive(self):
    return threading.Thread.is_alive(self)

  def join(self, timeout=None):
    return threading.Thread.join(self, timeout=timeout)

  def stop(self):
    self._m_stop_event.set()
    if self._m_run_process is not None and self._m_run_process.poll() is None:
      os.killpg(self._m_run_process.pid, signal.SIGTERM)

  def stopped(self):
    return self._m_stop_event.is_set()


class ArgumentParser(argparse.ArgumentParser):
  """A wrapper class of argparse.ArgumentParser
  which parse parameters from environment variables.

  """
  def __init__(self, **params):
    """Receive the same parameters as argparse.ArgumentParser"""
    super(self.__class__, self).__init__(**params)
    self._m_required_params = set()

  def add_argument(self, *args, **kwargs):
    parameter = self._get_optional_kwargs(*args, **kwargs)
    if parameter.get("required") is True:
      self._m_required_params.add(parameter["dest"])
      kwargs["required"] = False

    return super(self.__class__, self).add_argument(*args, **kwargs)

  def parse_known_args(self, args=None, namespace=None):
    args, argv = super(self.__class__, self).parse_known_args()

    required_params = copy.deepcopy(self._m_required_params)
    params = os.environ.get(__TASK_ENV_PARAMS__)
    if params is not None:
      params = json.loads(params)
      if not isinstance(params, dict):
        raise ValueError(
          "Environment variable '%s' must be a dict." % (__TASK_ENV_PARAMS__))
      for key, value in params.items():
        setattr(args, key, value)
        if key in required_params:
          required_params.remove(key)

    default_params = set()
    for param in required_params:
     if getattr(args, param) is not None:
       default_params.add(param)
    required_params -= default_params

    if len(required_params) > 0:
      raise ValueError("The following arguments are required: %s" % (
          ", ".join(required_params)))
    return args, argv
