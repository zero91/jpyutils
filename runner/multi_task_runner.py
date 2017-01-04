"""Tools for running a bunch of tasks which may contain dependency relationships. 
It can executing tasks efficiently in parallel if their dependency relations can be expressed 
as a topological graph.
"""

# Author: Donald Cheung <jianzhang9102@gmail.com>

import sys
import time
import signal
import re
import os

from .task_runner import TaskRunner
from .dependency_manager import TaskDependencyManager
from .progress_display import TableProgressDisplay
from .base import TaskStatus

class MultiTaskRunner(object):
    """A tasks manage class.

    It maintains a set of tasks, and run the tasks according to their topological relations.

    Parameters
    ----------
    log: string/None/subprocess.PIPE
        The file/directory which used to save logs. Default to be None, the process's file
        handles will be inherited from the parent. If log is a directory, log will be write
        to filename specified by each task's name end up with "stdout"/"stderr".

    render_arguments: dict
        Dict which used to replace tasks' parameter for its true value.

    parallel_degree: integer
        Parallel degree of this task. At most `parallel_degree' of the tasks will be run
        simultaneously.

    retry: integer
        Try executing each task retry times until succeed.
    """
    def __init__(self, log=None, render_arguments={}, parallel_degree=sys.maxint, retry=1):
        self.__log = log
        self.__render_arguments = render_arguments
        self.__parallel_degree = parallel_degree
        self.__retry = retry

        self.__dependency_manager = TaskDependencyManager()
        self.__task_runner_dict = dict()
        self.__running_task_set = set()
        self.__started = False

    def add(self, command, name=None, depends=None, **popen_kwargs):
        """Add a new task.

        Parameters
        ----------
        command: list/string
            The command which need to be executed.
            It has the same meaning as TaskRunner's command parameter.

        name: string
            The name of the task. Best using naming method of programming languages,
            for it may be used to create log files on disk.

        depends: string
            A string which is the concatenation of the names of all the tasks which must be
            executed ahead of this task. Separated by a single comma(',').

        popen_kwargs: dict
            It has the same meaning as Popen's arguments.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.
        """
        if name in self.__task_runner_dict:
            raise KeyError("Task {0} is already exists!".format(name))

        if isinstance(self.__log, str):
            if not os.path.exists(self.__log):
                os.makedirs(self.__log)

            if "stdout" not in popen_kwargs:
                popen_kwargs['stdout'] = open("%s/%s.stdout" % (self.__log, name), 'a+')

            if "stderr" not in popen_kwargs:
                popen_kwargs['stderr'] = open("%s/%s.stderr" % (self.__log, name), 'a+')
        else:
            popen_kwargs['stdout'] = self.__log
            popen_kwargs['stderr'] = self.__log

        runner = TaskRunner(command, name=name, retry=self.__retry, **popen_kwargs)
        self.__task_runner_dict[runner.name] = [TaskStatus.WAITING, runner]
        self.__dependency_manager.add_dependency(runner.name, depends)
        return self

    def adds(self, tasks_str, encoding="utf-8"):
        """Add tasks from a string.

        Parameters
        ----------
        tasks_str: string
            The string of the tasks, which is a python executable code.

        encoding: string
            The encode of the argument `tasks_str'.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.
        """
        tasks_str = tasks_str.decode(encoding)
        render_arg_pattern = re.compile(r"\<\%=(.*?)\%\>")
        for match_str in re.findall(render_arg_pattern, tasks_str):
            if match_str not in self.__render_arguments:
                raise KeyError("missing value for render argument {0}".format(match_str))

        def __lookup_func(self, reg_match):
            return self.__render_arguments[reg_match.group(1).strip()]
        tasks_str = render_arg_pattern.sub(__lookup_func, tasks_str)

        exec(tasks_str, {}, {'TaskRunner': self.add})
        return self

    def addf(self, tasks_fname, encoding="utf-8"):
        """Add tasks from a file.

        Parameters
        ----------
        tasks_fname : string
            The file's name of which contains tasks.

        encoding: string
            The encode of file content specified by `tasks_fname'.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.
        """
        return self.adds(open(tasks_fname, 'r').read(), encoding)

    def lists(self, displayer=None):
        """List all tasks.

        Parameters
        ----------
        displayer: class
            Class which can be used to display tasks.
        """
        if not self.__dependency_manager.is_topological():
            raise ValueError("tasks's depenency relationships is not topological")

        if displayer is None:
            TableProgressDisplay(self.__dependency_manager, self.__task_runner_dict).display()
        else:
            displayer(self.__dependency_manager, self.__task_runner_dict).display()

    def run(self, tasks=None, verbose=False, displayer=None):
        """Running all jobs of this task.

        Parameters
        ----------
        tasks : string
            The tasks which needed to be executed, sepecified by string seperated by one comma.
            Supported format is the same as method:
                    `dependency_manager.TaskDependencyManager.parse_tasks'

        Returns
        -------
        result : integer
            0 for success, otherwise nonzero.

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

        manager = self.__dependency_manager.parse_tasks(tasks)
        tasks_info = manager.get_tasks_info()[1]
        for task_name in self.__task_runner_dict:
            if task_name not in tasks_info:
                self.__task_runner_dict[task_name][0] = TaskStatus.DISABLED

        if displayer is None:
            displayer = TableProgressDisplay(self.__dependency_manager, self.__task_runner_dict)
        else:
            displayer = displayer(self.__dependency_manager, self.__task_runner_dict)
        verbose and displayer.display()

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

            verbose and displayer.display()
            if len(tasks_info) == 0 and len(self.__running_task_set) == 0 and len(ready_list) == 0:
                break
            time.sleep(0.1)

        verbose and displayer.display()
        return 0

    def __kill_signal_handler(self, signum, stack):
        self.terminate()
        TableProgressDisplay(self.__dependency_manager, self.__task_runner_dict).display()
        sys.stderr.write("\nReceive signal %d, all running processes are killed.\n" % signum)
        exit(1)

    def terminate(self):
        """Terminate all running process."""
        for task_name in self.__running_task_set.copy():
            self.__task_runner_dict[task_name][1].terminate()
            self.__task_runner_dict[task_name][0] = TaskStatus.KILLED
            self.__running_task_set.remove(task_name)
