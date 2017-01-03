"""Tools for running a simple task, or running a bunch of tasks which may contain dependency
relations. It can executing tasks efficiently in parallel if the dependency relationship
between them can be expressed as a topology graph.
"""
# Author: Donald Cheung <jianzhang9102@gmail.com>

import os
import subprocess
import threading
import time
import signal

class TaskRunner(threading.Thread):
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

    def run(self):
        self.__lock.acquire()
        if self.__started is True:
            raise RuntimeError("runner can only be started one time")
        else:
            self.__started = True
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
        return self.returncode

    def join(self, timeout=None):
        deadline = None
        if timeout is not None:
            deadline = time.time() + timeout

        while self.is_alive() and (deadline is None or time.time() < deadline):
            time.sleep(0.1)

    def terminate(self):
        self._stop.set()
        if self.__run_process is not None and self.__run_process.poll() is None:
            os.killpg(self.__run_process.pid, signal.SIGTERM)
        return True

