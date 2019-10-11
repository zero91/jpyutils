from lanfang.runner.base import Runner, SharedScope

import sys
import logging
import copy
import inspect
import multiprocessing
import signal


class ProcRunner(Runner, multiprocessing.Process):
  """Execute a callable object in an independent process.

  Parameters
  ----------
  target: callable
    A callable object to be invoked by the run() method.

  args: tuple
    The argument tuple for the target invocation. Defaults to ().

  kwargs: dict
    The dictionary of keyword arguments for the target invocation.
    Defaults to {}.

  Properties
  ----------
  daemon: Whether this process is daemon or not.

  """
  __doc__ += "\nDocument of Runner\n" + ("-" * 20) + "\n" + Runner.__doc__

  def __init__(self, target, *, name=None, args=(), kwargs={},
                             daemon=None, **runner_kwargs):
    if not callable(target):
      raise TypeError("Parameter 'target' must be callable object.")

    multiprocessing.Process.__init__(
      self, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)

    Runner.__init__(
      self, target=target, name=name, daemon=daemon,
      internal_scope=SharedScope.PROCESS, **runner_kwargs)

    self._m_name = self.name
    self._m_signal_handler = self._record_signal_handler()

  def _record_signal_handler(self):
    signal_handler = {}
    for signum in signal.Signals:
      signal_handler[signum] = signal.getsignal(signum)
    return signal_handler

  def _recover_signal_handler(self):
    for signum in signal.Signals:
      run_handler = signal.getsignal(signum)
      if self._m_signal_handler[signum] == run_handler:
        continue
      logging.info("Recover handler of signal %s from %s to %s",
          signum, run_handler, self._m_signal_handler[signum])
      signal.signal(signum, self._m_signal_handler[signum])

  def run(self):
    self._recover_signal_handler()
    for stream in ["stdin", "stdout", "stderr"]:
      stream_value = getattr(self, stream)
      if stream_value is not None:
        setattr(sys, stream, stream_value)
    return Runner.run(self)

  def _execute_target(self, input_params):
    try:
      if self._target:
        ret_value = self._target(**input_params)
      else:
        ret_value = None
      return 0, ret_value

    except SystemExit as se:
      if se.code != 0:
        logging.warning("Runner '%s' exit with code '%d' on attempts %d/%d",
            self._m_name, se.code, self._m_runner_status["attempts"],
            self._m_retry_limit)
      return se.code, None

    except BaseException as be:
      logging.warning("Runner '%s' got exception %s' on attempts %d/%d: %s",
          self._m_name, type(be),
          self._m_runner_status["attempts"], self._m_retry_limit, be)
      return 1, None

  def _fetch_input_params(self, shared_input_params):
    input_params = copy.deepcopy(shared_input_params)
    for param_name, param_value in zip(
            inspect.signature(self._target).parameters, self._args):
      input_params[param_name] = param_value

    for param_name, param_value in self._kwargs.items():
      input_params[param_name] = param_value

    return input_params

  def is_alive(self):
    return multiprocessing.Process.is_alive(self)

  def stop(self):
    self._m_runner_status["need_stop"] = True
    multiprocessing.Process.terminate(self)

  def stopped(self):
    return "need_stop" in self._m_runner_status
