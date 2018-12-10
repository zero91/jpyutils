"""Run a function in a separate process."""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import absolute_import

import sys
import multiprocessing
import logging
import datetime
import time

class ProcRunner(multiprocessing.Process):
    """Run a function in an independent process.

    Parameters
    ----------
    target: function
        target is the callable object to be invoked by the run() method.

    name: str
        The name of the process.

    retry: integer
        Try executing the target function 'retry' times until succeed, otherwise failed.

    interval: float
        Interval time between each try.

    stdin: file
        Input stream.

    stdout: file
        Output stream.

    stderr: file
        Error output stream.

    args: tuple
        The argument tuple for the target invocation. Defaults to ().

    kwargs: dict
        The dictionary of keyword arguments for the target invocation. Defaults to {}.

    Notes
    -----
    PorcRunner.stdin
        stdin attribute of the running process.

    ProcRunner.stdout
        stdout attribute of the running process.

    ProcRunner.stderr
        stderr attribute of the running process.

    ProcRunner.exitcode
        Attribute which specify the exit code of the target.

    """
    def __init__(self, target, name=None, retry=1, interval=5,
                               stdin=None, stdout=None, stderr=None, args=(), kwargs={}):
        super(__class__, self).__init__(target=target, name=name, args=args, kwargs=kwargs)
        self.daemon = True
        self._m_name = name

        if stdin is None:
            self.stdin = sys.stdin
        else:
            self.stdin = stdin

        if stdout is None:
            self.stdout = sys.stdout
        else:
            self.stdout = stdout

        if stderr is None:
            self.stderr = sys.stderr
        else:
            self.stderr = stderr

        self._m_lock = multiprocessing.Lock()
        self._m_try_num = multiprocessing.Value('i', 0)
        self._m_retry_limit = retry
        self._m_retry_interval = interval
        self._m_start_time = multiprocessing.Value('d', 0)
        self._m_elapsed_time = multiprocessing.Value('d', 0)

    def run(self):
        self._m_lock.acquire()
        if self._m_start_time.value > 0:
            raise RuntimeError("Instance can be started only once")
        self._m_start_time.value = datetime.datetime.now().timestamp()
        self._m_lock.release()

        sys.stdin = self.stdin
        sys.stdout = self.stdout
        sys.stderr = self.stderr

        while self._m_try_num.value < self._m_retry_limit:
            if self._m_try_num.value > 0:
                time.sleep(self._m_retry_interval)
            self._m_try_num.value += 1
            last_exitcode = 0
            try:
                super(__class__, self).run()
                break
            except SystemExit as se:
                if se.code == 0:
                    break
                logging.warning("Proc '%s' exit with non-zero code '%d' on try %d/%d" % (
                        self._m_name, se.code, self._m_try_num.value, self._m_retry_limit))
                last_exitcode = se.code
            except Exception as e:
                logging.warning("Proc '%s' got exception '%s' on try %d/%d" % (
                        self._m_name, e, self._m_try_num.value, self._m_retry_limit))
                last_exitcode = 1

        self._m_elapsed_time.value = datetime.datetime.now().timestamp() - self._m_start_time.value
        exit(last_exitcode)

    @property
    def info(self):
        """Get task's internal information.

        Returns
        -------
        information: dict
            A dict which contains task's information, including:

                elapsed_time : integer, time since task started.
                start_time : datetime.datetime, started time of current task.
                exitcode : int, task's exit code
                try: string, task's try information.
        """
        if self.is_alive() and self._m_start_time.value > 0:
            self._m_elapsed_time.value = \
                    datetime.datetime.now().timestamp() - self._m_start_time.value

        if self._m_start_time.value > 0:
            start_time = datetime.datetime.fromtimestamp(self._m_start_time.value)
        else:
            start_time = None

        return {
            'elapsed_time': self._m_elapsed_time.value,
            'start_time': start_time,
            'exitcode': self.exitcode,
            'try': "%s/%s" % (self._m_try_num.value, self._m_retry_limit)
        }

