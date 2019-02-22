import sys
import logging
import time
import copy
import multiprocessing
from multiprocessing import managers


class ProcRunner(multiprocessing.Process):
    """Execute a callable object in an independent process.

    Parameters
    ----------
    target: callable
        A callable object to be invoked by the run() method.

    name: str
        The name of the process.

    args: tuple
        The argument tuple for the target invocation. Defaults to ().

    kwargs: dict
        The dictionary of keyword arguments for the target invocation. Defaults to {}.

    stdin: file
        Input stream.

    stdout: file
        Output stream.

    stderr: file
        Error output stream.

    retry: integer
        Try executing the target function 'retry' times until succeed, otherwise failed.

    interval: float
        Interval time between each try.

    daemon: boolean
        A boolean value indicating whether this runner is a daemon process (True) or not (False).

    pre_hook: callable
        A callable object to be invoked before 'target' execution.
        The input parameters is the same as 'target'.

    post_hook: callable
        A callable object to be invoked after 'target' execution.
        The input parameters is the return value of calling 'target'.

    share_dict: multiprocessing.managers.DictProxy
        A multiple process sharing dictionary which can be used to exchange data between processes.
        Sample creatation method:

            with Manager() as manager:
                share_dict = manager.dict()

    Properties
    ----------
    daemon: Whether this process is daemon or not.

    name: The name of this process.

    stdin: stdin attribute of the running process.

    stdout: stdout attribute of the running process.

    stderr: stderr attribute of the running process.

    exitcode: The exit code after execute target.

    """
    def __init__(self, target, name=None, args=(), kwargs={}, stdin=None, stdout=None, stderr=None,
                               retry=1, interval=5, daemon=True,
                               pre_hook=None, post_hook=None,
                               share_dict=None):
        if not callable(target):
            raise TypeError("Parameter 'target' should be callable")

        if share_dict is not None and \
                not isinstance(share_dict, multiprocessing.managers.DictProxy):
            raise TypeError("Parameter 'share_dict' should be type " \
                            "multiprocessing.managers.DictProxy")

        super(__class__, self).__init__(target=target, name=name, args=args, kwargs=kwargs)
        self.daemon = daemon

        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        self._m_retry_limit = retry
        self._m_retry_interval = interval

        self._m_pre_hook = pre_hook
        self._m_post_hook = post_hook

        self._m_lock = multiprocessing.Lock()
        self._m_share_dict = share_dict
        self._m_manager = multiprocessing.Manager()
        self._m_proc_info = self._m_manager.dict({"try_num": 0})

    def run(self):
        self._m_lock.acquire()
        if self._m_proc_info.get("start_time", 0) > 0:
            raise RuntimeError("cannot start a process twice")
        self._m_proc_info["start_time"] = time.time()
        self._m_lock.release()

        for stream in ["stdin", "stdout", "stderr"]:
            stream_value = getattr(self, stream)
            if stream_value is not None:
                setattr(sys, stream, stream_value)

        if self._m_pre_hook is not None and self._m_pre_hook(*self._args, **self._kwargs) != 0:
            raise RuntimeError("Execute pre_hook failed, please check the input parameters")

        target_ret_value = None
        last_exitcode = None
        while self._m_proc_info["try_num"] < self._m_retry_limit:
            if self._m_proc_info["try_num"] > 0:
                time.sleep(self._m_retry_interval)
            self._m_proc_info["try_num"] += 1

            last_exitcode = 0
            try:
                if self._target:
                    target_ret_value = self._target(*self._args, **self._kwargs)
                break
            except SystemExit as se:
                if se.code == 0:
                    break
                logging.warning("Proc '%s' exit with non-zero code '%d' on try %d/%d" % (
                        self.name, se.code, self._m_proc_info["try_num"], self._m_retry_limit))
                last_exitcode = se.code
            except BaseException as be:
                logging.warning("Proc '%s' got exception '%s' on try %d/%d" % (
                        self.name, be, self._m_proc_info["try_num"], self._m_retry_limit))
                last_exitcode = 1

        if last_exitcode != 0:
            exit(last_exitcode)

        if self._m_post_hook is not None and \
                self._m_post_hook(copy.deepcopy(target_ret_value)) != 0:
            raise RuntimeError("Execute post_hook failed, please check the output values")

        self._m_proc_info["return"] = copy.deepcopy(target_ret_value)
        if self._m_share_dict is not None:
            self._m_share_dict[self.name] = copy.deepcopy(target_ret_value)
        self._m_proc_info["elapsed_time"] = time.time() - self._m_proc_info["start_time"]
        exit(0)

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
                    return: return value of the task,
                    try: try information of the task,
                }

        """
        if self.is_alive() and "start_time" in self._m_proc_info:
            self._m_proc_info["elapsed_time"] = time.time() - self._m_proc_info["start_time"]

        return {
            "start_time": self._m_proc_info.get("start_time"),
            "elapsed_time": self._m_proc_info.get("elapsed_time"),
            "exitcode": self.exitcode,
            "return": self._m_proc_info.get("return"),
            "try": "%s/%s" % (self._m_proc_info["try_num"], self._m_retry_limit)
        }
