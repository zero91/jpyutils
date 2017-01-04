"""Running a simple task."""

# Author: Donald Cheung <jianzhang9102@gmail.com>

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
    command: list/string
        The command which need to be executed.

        `command' should be a sequence of program arguments or else a single string. By default,
        the program to execute is the first item in `command' if `command' is a sequence.

        The shell argument (which defaults to False) specifies whether to use the shell as the
        program to execute. If shell is True, it is recommended to pass command as a string rather
        than as a sequence.

    name: string
        The name of the task. Best using naming method of programming languages.

    retry: integer
        Try executing the command `retry' times until succeed, otherwise failed.

    Notes
    -----
    TaskRunner.command
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
    def __init__(self, command, name=None, retry=1, **popen_kwargs):
        threading.Thread.__init__(self, name=name)
        self.setDaemon(True)

        self.command = command
        self.returncode = None
        self.stdin = None
        self.stdout = None
        self.stderr = None

        self._stop = threading.Event()

        self.__popen_kwargs = popen_kwargs
        self.__run_process = None
        self.__try_num = 0
        self.__retry_limit = retry
        self.__lock = threading.Lock()
        self.__started = False

        self.__start_time = None
        self.__elapse_time = datetime.timedelta(0, 0, 0)

    def run(self):
        self.__lock.acquire()
        if self.__started is True:
            raise RuntimeError("runner can only be started one time")
        else:
            self.__started = True
            self.__start_time = datetime.datetime.now()
        self.__lock.release()

        if "preexec_fn" in self.__popen_kwargs:
            raise RuntimeError("preexec_fn argument not allowed, it will be overridden.")
        self.__popen_kwargs['preexec_fn'] = os.setsid

        if "close_fds" not in self.__popen_kwargs:
            self.__popen_kwargs["close_fds"] = True

        last_returncode = None
        while not self._stop.is_set() and self.__try_num < self.__retry_limit:
            self.__try_num += 1
            self.__run_process = subprocess.Popen(self.command, **self.__popen_kwargs)

            self.stdin = self.__run_process.stdin
            self.stderr = self.__run_process.stderr
            self.stdout = self.__run_process.stdout
            while self.__run_process.poll() is None:
                time.sleep(0.1)

            last_returncode = self.__run_process.poll()
            if last_returncode == 0:
                break

        self.returncode = last_returncode
        self.__elapse_time = datetime.datetime.now() - self.__start_time
        return self.returncode

    def join(self, timeout=None):
        """Waiting current process to be finished. """
        deadline = None
        if timeout is not None:
            deadline = time.time() + timeout

        while self.is_alive() and (deadline is None or time.time() < deadline):
            time.sleep(0.1)

    def terminate(self):
        """Terminate current running process."""
        self._stop.set()
        if self.__run_process is not None and self.__run_process.poll() is None:
            os.killpg(self.__run_process.pid, signal.SIGTERM)
        return True

    def get_info(self):
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
        if self.is_alive() and self.__start_time is not None:
            self.__elapse_time = datetime.datetime.now() - self.__start_time

        elapse_time = self.__elapse_time.total_seconds()
        elapse_time += self.__elapse_time.microseconds / 1000000
        return { 'elapse_time' : elapse_time,
                 'start_time' : self.__start_time,
                 'returncode' : self.returncode,
                 'try_info' : "%s/%s" % (self.__try_num, self.__retry_limit) }

