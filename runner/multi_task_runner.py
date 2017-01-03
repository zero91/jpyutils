"""Tools for running a simple task, or running a bunch of tasks which may contain dependency
relations. It can executing tasks efficiently in parallel if the dependency relationship
between them can be expressed as a topology graph.
"""
# Author: Donald Cheung <jianzhang9102@gmail.com>

import sys
import multiprocessing
import time
import datetime
import signal
import re

from task_runner import TaskRunner
from dependency_manager import TaskDependencyManager

class TaskStatus(object):
    DISABLED = 0
    WAITING  = 1
    READY    = 2
    RUNNING  = 3
    DONE     = 4
    FAILED   = 5
    KILLED   = 6


class MultiTaskRunner(multiprocessing.Process):
    def __init__(self, log=None, render_arguments={}, parallel_degree=sys.maxint, retry=1):
        multiprocessing.Process.__init__(self)

        self.__log = log
        self.__render_arguments = render_arguments
        self.__parallel_degree = parallel_degree
        self.__retry = retry

        self.__dependency_manager = TaskDependencyManager()
        self.__task_runner_dict = dict()
        self.__running_task_set = set()
        self.__started = False

    def add(self, command, name=None, depends=None, **popen_kwargs):
        if name in self.__task_runner_dict:
            raise KeyError("Task {0} is already exists!".format(name))

        runner = TaskRunner(command, name=name, **popen_kwargs)
        self.__task_runner_dict[runner.name] = [TaskStatus.WAITING, runner]
        self.__dependency_manager.add_dependency(runner.name, depends)
        return self

    def adds(self, task_str, encoding="utf-8"):
        task_str = task_str.decode(encoding)
        render_arg_pattern = re.compile(r"\<\%=(.*?)\%\>")
        for match_str in re.findall(render_arg_pattern, task_str):
            if match_str not in self.__render_arguments:
                raise KeyError("missing value for render argument {0}".format(match_str))

        def __lookup_func(self, reg_match):
            return self.__render_arguments[reg_match.group(1).strip()]
        task_str = render_arg_pattern.sub(__lookup_func, task_str)

        exec(task_str, {}, {'TaskRunner': self.add})
        return self

    def addf(self, tasks_fname, encoding="utf-8"):
        return self.adds(open(tasks_fname, 'r').read(), encoding)

    def lists(self):
        if not self.__dependency_manager.is_topological():
            raise ValueError("tasks's depenency relationships is not topological")
        #_MultiTaskProgressDisplay(self.__task_id, dict(self.__task_depends_list), self.__task_runner).display()

    def run(self, tasks=None, verbose=False):
        """Running all jobs of this task.

        Parameters
        ----------
        tasks : string
            The tasks which needed to be executed, sepecified by topological ids,
            format like "-3,5,7-10-2,13-16,19-".

        Returns
        -------
        result : integer
            0 for success, otherwise -1

        Notes
        -----
            Should only be executed once.
        """
        if self.__started:
            raise RuntimeError("{0} should be executed only once".format(self.__class__.__name__))

        if not self.__dependency_manager.is_topological():
            raise RuntimeError("tasks' dependency relationships is not topological")

        self.__started = True

        signal.signal(signal.SIGINT, self.__kill_signal_handler)
        signal.signal(signal.SIGTERM, self.__kill_signal_handler)

        tasks_info = self.__dependency_manager.parse_tasks(tasks).get_tasks_info()[1]
        for task_name in self.__task_runner_dict:
            if task_name not in tasks_info:
                self.__task_runner_dict[task_name][0] = TaskStatus.DISABLED

        ready_list = list()
        while True:
            for task_name, (initial_id, running_id, depends, rdepends) in tasks_info.iteritems():
                if len(depends) == 0 and \
                                    self.__task_runner_dict[task_name][0] == TaskStatus.WAITING:
                    ready_list.append(task_name)
                    self.__task_runner_dict[task_name][0] = TaskStatus.READY

            ready_list.sort(key=lambda task_name: tasks_info[task_name][1])
            while len(self.__running_task_set) < self.__parallel_degree and len(ready_list) > 0:
                task_name = ready_list.pop(0)
                self.__running_task_set.add(task_name)
                self.__task_runner_dict[task_name][0] = TaskStatus.RUNNING
                self.__task_runner_dict[task_name][1].start()

            for task_name in self.__running_task_set.copy():
                if self.__task_runner_dict[task_name][1].is_alive():
                    continue
                self.__running_task_set.remove(task_name)

                ret_code = self.__task_runner_dict[task_name][1].returncode
                if ret_code != 0:
                    self.__task_runner_dict[task_name][0] = TaskStatus.FAILED
                    self.terminate()
                    sys.stderr.write("Task {0} failed, exit code {1}\n" % (task_name, ret_code))
                    return 1
                else:
                    self.__task_runner_dict[task_name][0] = TaskStatus.DONE

                for depend_task_name in tasks_info[task_name][3]:
                    tasks_info[depend_task_name][2].remove(task_name)
                tasks_info.pop(task_name)

            if len(tasks_info) == 0 and len(self.__running_task_set) == 0 and len(ready_list) == 0:
                break
            time.sleep(0.1)
        return 0

    def __kill_signal_handler(self, signum, stack):
        self.terminate()
        sys.stderr.write("\nReceive signal %d, all running processes are killed.\n" % signum)
        exit(1)

    def terminate(self):
        for task_name in self.__running_task_set.copy():
            self.__task_runner_dict[task_name][1].terminate()
            self.__task_runner_dict[task_name][0] = TaskStatus.KILLED
            self.__running_task_set.remove(task_name)


if __name__ == "__main__":
    runner = MultiTaskRunner(parallel_degree=100)
    #runner = MultiTaskRunner()
    runner.add(["ls", "-l", "/Users/zero91"], name="ls_home", depends="ls")
    runner.add(["ls", "-l", "/Users/zero91/Documents"], name="documents", depends="ls")
    runner.add(["ls", "-l", "/Users/zero91/workspace"], name="workspace", depends="ls_home")
    runner.add(["ls", "-l", "/Users/zero91/Downloads"], name="downloads", depends="workspace")
    runner.add(["ls", "-l", "/Users/zero91/Applications"], name="applications", depends="downloads")
    runner.add(["ls", "-l"], name="ls")

    #runner.lists()

    #runner.run()
    runner.run("0-3")
    #runner.start()
    print 'hello'
