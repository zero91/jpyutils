"""Running a simple task in a separate process.

Manage the created process, and retry at least 'retry' times util succeed.
"""
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import subprocess
import threading
import datetime
import time
import signal

class TaskRunner(threading.Thread):
    """Maintaining a task in a python thread.

    Running task in a independent process.

    The full function signature is largely the same as that of the Popen constructor,
    except that preexec_fn is not permitted as it is used internally.
    All other supplied arguments are passed directly through to the Popen constructor.

    Parameters
    ----------
    cmd: list/str
        The command which need to be executed.

        `cmd' should be a sequence of program arguments or else a single string. By default,
        the program to execute is the first item in `cmd' if `cmd' is a sequence.

        The shell argument (which defaults to False) specifies whether to use the shell as the
        program to execute. If shell is True, it is recommended to pass cmd as a string rather
        than as a sequence.

    name: str
        The name of the task. Best using naming method of programming languages.

    retry: integer
        Try executing the cmd `retry' times until succeed, otherwise failed.

    Notes
    -----
    TaskRunner.cmd
        The command which need to be executed.

    TaskRunner.stdin
        stdin attribute of the running process.

    TaskRunner.stdout
        stdout attribute of the running process.

    TaskRunner.stderr
        stderr attribute of the running process.

    TaskRunner.returncode
        Attribute which specify the exit code of this task.

    """
    def __init__(self, cmd, name=None, retry=1, **popen_kwargs):
        threading.Thread.__init__(self, name=name)
        self.daemon = True

        self.cmd = cmd
        self.returncode = None
        self.stdin = None
        self.stdout = None
        self.stderr = None

        self._m_started = False
        self._m_stop_flag = threading.Event()
        self._m_lock = threading.Lock()
        self._m_run_process = None

        self._m_popen_kwargs = popen_kwargs
        self._m_try_num = 0
        self._m_retry_limit = retry
        self._m_start_time = None
        self._m_elapse_time = datetime.timedelta(0, 0, 0)

    def __del__(self):
        if self._m_run_process is not None:
            if self._m_run_process.stdin is not None:
                self._m_run_process.stdin.close()

            if self._m_run_process.stdout is not None:
                self._m_run_process.stdout.close()

            if self._m_run_process.stderr is not None:
                self._m_run_process.stderr.close()

    def run(self):
        self._m_lock.acquire()
        if self._m_started is True:
            raise RuntimeError("Instance can be started only once")
        else:
            self._m_started = True
            self._m_start_time = datetime.datetime.now()
        self._m_lock.release()

        if "preexec_fn" in self._m_popen_kwargs:
            raise ValueError("Argument `preexec_fn` is not allowed, it will be used internally.")
        self._m_popen_kwargs['preexec_fn'] = os.setsid

        if "close_fds" not in self._m_popen_kwargs:
            self._m_popen_kwargs["close_fds"] = True

        last_returncode = None
        while not self._m_stop_flag.is_set() and self._m_try_num < self._m_retry_limit:
            self._m_try_num += 1
            self._m_run_process = subprocess.Popen(self.cmd, **self._m_popen_kwargs)

            self.stdin = self._m_run_process.stdin
            self.stderr = self._m_run_process.stderr
            self.stdout = self._m_run_process.stdout
            while self._m_run_process.poll() is None:
                time.sleep(0.1)

            last_returncode = self._m_run_process.poll()
            if last_returncode == 0:
                break

        self.returncode = last_returncode
        self._m_elapse_time = datetime.datetime.now() - self._m_start_time
        return self.returncode

    def terminate(self):
        """Terminate current running process."""
        self._m_stop_flag.set()
        if self._m_run_process is not None and self._m_run_process.poll() is None:
            os.killpg(self._m_run_process.pid, signal.SIGTERM)
        return True

    @property
    def info(self):
        """Get task's internal information.

        Returns
        -------
        information: dict
            A dict which contains task's information, including:

                elapse_time : integer, time since task started.
                start_time : datetime.datetime, started time of current task.
                returncode : int, task's exit code
                try_info : string, task's try information.
        """
        if self.is_alive() and self._m_start_time is not None:
            self._m_elapse_time = datetime.datetime.now() - self._m_start_time

        elapse_time = self._m_elapse_time.total_seconds()
        elapse_time += self._m_elapse_time.microseconds / 1000000.
        return {
            'elapse_time': elapse_time,
            'start_time': self._m_start_time,
            'returncode': self.returncode,
            'try_info': "%s/%s" % (self._m_try_num, self._m_retry_limit)
        }

