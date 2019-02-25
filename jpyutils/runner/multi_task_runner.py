from .common import TaskStatus
from .task_runner import TaskRunner
from .proc_runner import ProcRunner
from .progress_display import TableProgressDisplay

import time
import signal
import re
import os
import collections
import copy
import json
import logging
import hashlib
import yaml
import multiprocessing


class DependencyManager(object):
    """Manage topological relations for a bunch of tasks.
    """
    def __init__(self):
        self._m_task_info = collections.defaultdict(dict)
        self._m_task_initial_id = 0
        self._m_is_valid = None

    @classmethod
    def from_data(cls, task_list):
        """Create an instance from a bunch of tasks.

        Parameters
        ----------
        task_list: list
            A sequence of tasks, each item is in format (task_name, depends).

        Returns
        -------
        instance: DependencyManager
            Instance constructed from input argument.

        """
        instance = cls()
        for task_name, depends in task_list:
            instance.add(task_name, depends)
        return instance

    def add(self, task_name, depends=None):
        """Add dependency relation for a task.

        Parameters
        ----------
        task_name: str
            Task name.

        depends: string, list, tuple, set, dict
            Task names which need to be done before 'task_name'.

        Returns
        -------
        instance: DependencyManager
            Current instance's reference.

        Raises
        ------
        TypeError: Type of 'depends' is not supported.

        """
        if depends is None or len(depends) == 0:
            depends = set()

        elif isinstance(depends, str):
            depends = set(map(str.strip, depends.split(',')))

        elif isinstance(depends, (list, tuple, set, dict)):
            depends = set(depends)

        else:
            raise TypeError("Parameter 'depends' must be a string or list")

        for name in list(depends) + [task_name]:
            if name not in self._m_task_info:
                self._m_task_info[name] = {
                    "initial_id": None,
                    "run_id": None,
                    "depends": set(),
                    "reverse_depends": set(),
                }

        self._m_is_valid = None
        if self._m_task_info[task_name]["initial_id"] is None:
            self._m_task_info[task_name]["initial_id"] = self._m_task_initial_id
            self._m_task_initial_id += 1
        self._m_task_info[task_name]["depends"] |= depends

        for name in depends:
            self._m_task_info[name]["reverse_depends"].add(task_name)

    def get(self, task_name):
        """Get dependency tasks.

        Parameters
        ----------
        task_name: string
            Name of a task.

        Returns
        -------
        depends: set
            Set of tasks which need to be done before 'task_name'.

        Raises
        ------
        KeyError: Can't find the task.

        """
        if task_name not in self._m_task_info:
            raise KeyError("Cannot find task '%s'" % (task_name))
        return copy.deepcopy(self._m_task_info[task_name]["depends"])

    def get_task_info(self):
        """Get information of all tasks.

        Returns
        -------
        task_info: collections.defaultdict
            {
                "task_name": {
                    "initial_id": 0,
                    "run_id": 1,
                    "depends": set(),
                    "reverse_depends": set(),
                },
            }

        """
        if not self.is_topological():
            raise ValueError("The dependency relations is not topological")
        return copy.deepcopy(self._m_task_info)

    def is_topological(self):
        """Assess whether current relations is topological.

        Returns
        -------
        is_topological: boolean
            True if current relations is topological, otherwise False.

        """
        if self._m_is_valid is not None:
            return self._m_is_valid

        if self._m_task_initial_id != len(self._m_task_info):
            self._m_is_valid = False
            return self._m_is_valid

        task_info = copy.deepcopy(self._m_task_info)
        cur_task_id = 0
        while len(task_info) > 0:
            ready_list = list()
            for name in task_info:
                if len(task_info[name]["depends"]) == 0:
                    ready_list.append(name)

            if len(ready_list) == 0:
                self._m_is_valid = False
                return self._m_is_valid

            for name in sorted(ready_list, key=lambda name: task_info[name]["initial_id"]):
                self._m_task_info[name]["run_id"] = cur_task_id
                cur_task_id += 1
                for depend_name in task_info[name]["reverse_depends"]:
                    task_info[depend_name]["depends"].remove(name)
                task_info.pop(name)

        self._m_is_valid = True
        return self._m_is_valid

    def __parse_single_task_id(self, task_str):
        if task_str in self._m_task_info:
            return self._m_task_info[task_str]["run_id"]

        elif task_str.isdigit():
            return int(task_str)

        elif len(task_str) > 0:
            raise ValueError("Can't find task '{0}' or its format does not been supported".format(
                            task_str))
        return None

    def parse_tasks(self, tasks_str):
        """Parse a string into full tasks with its dependency relations.

        Parameters
        ----------
        tasks_str: string
            Suppose we have a batch of tasks as [(0, 'a'), (1, 'b'), (2, 'c'), ..., (25, 'z')].

            'tasks_str' support the following formats:
            (1) "-3,5,7-10-2,13-16,19-".
                "-3" means range from 0(start) to 3, which is "0,1,2,3".
                "7-10-2" means range from 7 to 10, step length 2, which is "7,9".
                "13-16" means range from 13 to 16, step length 1, which is "13,14,15,16."
                "19-" mean range from 19 to 25(end), which is "19,20,21,22,23,24,25".

            (2) "1-4,x,y,z"
                "1-4" mean range from 1 to 4, which is "1,2,3,4"
                "x" mean task 'x', task id is 23.
                "y" mean task 'y', task id is 24.
                "z" mean task 'z', task id is 25.
                So, above string mean jobs "1,2,3,4,23,24,25".

        Returns
        -------
        dependency_manager: DependencyManager
            DependencyManager which contains all the jobs specified by input argument.

        """
        if not self.is_topological():
            raise ValueError("The dependency relations is not topological")

        if tasks_str is None:
            return copy.deepcopy(self)

        tasks_set = set()
        for seg in tasks_str.split(','):
            seg = seg.strip()
            if seg == "":
                continue

            item_list = seg.split('-')
            if len(item_list) == 1:
                tid = self.__parse_single_task_id(item_list[0])
                if tid is not None and 0 <= tid < len(self._m_task_info):
                    tasks_set.add(tid)
                else:
                    logging.warning("Invalid task id '%s'" % (seg))

            elif 2 <= len(item_list) <=  3:
                start = self.__parse_single_task_id(item_list[0])
                stop = self.__parse_single_task_id(item_list[1])
                if len(item_list) == 3:
                    step = self.__parse_single_task_id(item_list[2])
                else:
                    step = 1

                if start is None:
                    start = 0
                if stop is None:
                    stop = len(self._m_task_info) - 1
                if step is None:
                    step = 1
                tasks_set |= set(range(start, stop + 1, step))

            else:
                raise ValueError("format of the task str '{0}' does not support".format(seg))

        valid_tasks_list = list(filter(lambda t: self._m_task_info[t]["run_id"] in tasks_set,
                                       self._m_task_info))
        valid_tasks_list.sort(key=lambda t: self._m_task_info[t]["initial_id"])

        valid_tasks_set = set(valid_tasks_list)
        valid_tasks_depends = list()
        for task in valid_tasks_list:
            valid_tasks_depends.append(valid_tasks_set & self._m_task_info[task]["depends"])
        return self.__class__.from_data(zip(valid_tasks_list, valid_tasks_depends))


class MultiTaskRunner(object):
    """Run a bunch of tasks.

    It maintains a bunch of tasks, and run the tasks according to their topological relations.

    Parameters
    ----------
    log_path: str
        The directory which used to save logs.
        Default to be None, the process's file handles will be inherited from the parent.
        Log will be write to filename specified by each task's name end up with "stdout"/"stderr".

    parallel_degree: integer
        Parallel degree of this task.
        At most 'parallel_degree' of the tasks will be executed simultaneously.

    retry: integer
        Try executing each task retry times until succeed.

    interval: float
        Interval time between each try.

    render_arguments: dict
        Dict which used to replace tasks parameter for its true value.
        Used for add task from string.

    displayer: class
        Class which can display tasks information.

    """
    def __init__(self, log_path=None, parallel_degree=-1, retry=1, interval=5, 
                       config=None, render_arguments=None, displayer=None):
        if log_path is not None:
            self._m_log_path = os.path.realpath(log_path)
        else:
            self._m_log_path = None

        self._m_parallel_degree = parallel_degree
        self._m_retry = retry
        self._m_interval = interval

        if render_arguments is None:
            self._m_render_arguments = dict()
        elif isinstance(render_arguments, dict):
            self._m_render_arguments = render_arguments
        else:
            raise ValueError("Parameter 'render_arguments' shoule be a dictionary")
        self._m_render_arg_pattern = re.compile(r"\<\%=(.*?)\%\>")

        self._m_dependency_manager = DependencyManager()
        self._m_open_file_list = list()
        self._m_runner_dict = collections.defaultdict(dict)
        self._m_running_tasks = set()
        self._m_started = False

        if config is not None:
            self._m_config = MultiTaskConfig(config)
        else:
            self._m_config = None

        if displayer is None:
            self._m_displayer_class = TableProgressDisplay
        else:
            self._m_displayer_class = displayer

    def __del__(self):
        for f in self._m_open_file_list:
            f.close()

    def add(self, target, name=None, args=(), kwargs={}, pre_hook=None, post_hook=None,
                                     depends=None, encoding="utf-8", daemon=True,
                                     append_log=False, **popen_kwargs):
        """Add a new task.

        Parameters
        ----------
        target: list, str, callable object
            The target which needs to be executed.

        name: str
            The name of the task. Best using naming method of programming languages,
            for it may be used for creating log files on disk.

        args: tuple
            The argument tuple for the target invocation. Defaults to ().

        kwargs: dict
            The dictionary of keyword arguments for the target invocation. Defaults to {}.

        pre_hook: callable
            A callable object to be invoked before 'target' execution.
            The input parameters is the same as 'target'.

        post_hook: callable
            A callable object to be invoked after 'target' execution.
            The input parameters is the return value of calling 'target'.

        depends: str, list, set, dict
            List of depended tasks.
            If this is a string, multiple tasks can be separated by a single comma(',').

        encoding: str
            The encoding of the data output by target's stdout.

        daemon: boolean
            A boolean value indicating whether this runner
            is a daemon process (True) or not (False).

        append_log: boolean
            Append logs to log file if set True, otherwise clear old content.

        popen_kwargs: dict
            It has the same meaning as arguments of Popen.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.

        Raises
        ------
        KeyError: If the task is already exists.

        """
        if name in self._m_runner_dict:
            raise KeyError("Task {0} is already exists!".format(name))

        if isinstance(self._m_log_path, str):
            if not os.path.exists(self._m_log_path):
                os.makedirs(self._m_log_path)

            for sname in ["stdout", "stderr"]:
                if sname in popen_kwargs:
                    logging.warning("Parameter '%s' already exists for task '%s'" % (sname, name))
                    continue
                open_tag = 'a+' if append_log is True else 'w+'
                popen_kwargs[sname] = open("%s/%s.%s" % (self._m_log_path, name, sname), open_tag)
                self._m_open_file_list.append(popen_kwargs[sname])

        share_config = None
        share_config_lock = None
        if self._m_config is not None:
            share_config = self._m_config.share_config
            share_config_lock = self._m_config.share_config_lock

        if callable(target):
            runner = ProcRunner(target, name=name, args=args, kwargs=kwargs,
                                        retry=self._m_retry,
                                        interval=self._m_interval,
                                        daemon=daemon,
                                        pre_hook=pre_hook,
                                        post_hook=post_hook,
                                        share_dict=share_config,
                                        share_dict_lock=share_config_lock,
                                        **popen_kwargs)
        else:
            runner = TaskRunner(target, name=name, retry=self._m_retry,
                                        interval=self._m_interval,
                                        daemon=daemon,
                                        pre_hook=pre_hook,
                                        post_hook=post_hook,
                                        share_dict=share_config,
                                        share_dict_lock=share_config_lock,
                                        encoding=encoding,
                                        **popen_kwargs)
        self._m_runner_dict[runner.name] = {
            "status": TaskStatus.WAITING,
            "runner": runner,
        }
        self._m_dependency_manager.add(runner.name, depends)
        return self

    def adds(self, tasks_str):
        """Add tasks from a string.

        Parameters
        ----------
        tasks_str: str
            The string of the tasks, which is a python executable code.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.

        Raises
        ------
        KeyError: Some of the arguments specified in 'tasks_str' did not provided.

        """
        exec(self._render_arguments(tasks_str), {}, {'Runner': self.add})
        return self

    def addf(self, tasks_fname, encoding="utf-8"):
        """Add tasks from a file.

        Parameters
        ----------
        tasks_fname: str
            The file's name of which contains tasks.

        encoding: str
            The encode of file content specified by `tasks_fname'.

        Returns
        -------
        instance: MultiTaskRunner
            Reference for current instance.

        """
        with open(tasks_fname, mode='r', encoding=encoding) as ftask:
            return self.adds(self._render_arguments(ftask.read()))

    def lists(self, display=True):
        """List all tasks.

        Parameters
        ----------
        display: boolean
            Set True if you want to display the tasks info on the screen.

        Returns
        -------
        taskslist: list
            The list of tasks in order of running id.

        Raises
        ------
        ValueError: If the tasks relations is not topological.

        """
        if not self._m_dependency_manager.is_topological():
            raise ValueError("The dependency relations of tasks is not topological")

        if display is True:
            self._m_displayer_class(self._m_dependency_manager, self._m_runner_dict).display()

        task_info = self._m_dependency_manager.get_task_info()
        return sorted(task_info, key=lambda name: task_info[name]["run_id"])

    def run(self, tasks=None, verbose=False, try_best=False):
        """Run a bunch of tasks.

        Parameters
        ----------
        tasks: str
            The tasks which needed to be executed,
            see more details of 'DependencyManager.parse_tasks'.

        verbose: boolean
            Print verbose information.

        try_best: boolean
            Set true if you want to executed the tasks as many as possible
            even if there exist some failed tasks.

        Returns
        -------
        result : integer
            0 for success, otherwise nonzero.

        Raises
        ------
        RuntimeError: If the set of tasks is not topological or has been exected already.

        Notes
        -----
        Should only be executed only once.

        """
        if self._m_started:
            raise RuntimeError("cannot run %s twice" % (self.__class__.__name__))

        if not self._m_dependency_manager.is_topological():
            raise RuntimeError("Dependency relations of tasks is not topological")

        self._m_started = True
        signal.signal(signal.SIGINT, self.__kill_signal_handler)
        signal.signal(signal.SIGTERM, self.__kill_signal_handler)

        running_task_info = self._m_dependency_manager.parse_tasks(tasks).get_task_info()
        for task_name in self._m_runner_dict:
            if task_name not in running_task_info:
                self._m_runner_dict[task_name]["status"] = TaskStatus.DISABLED

        if verbose:
            disp = self._m_displayer_class(self._m_dependency_manager, self._m_runner_dict)
            disp.display()
        else:
            disp = None

        ready_list = list()
        failed_task_num = 0
        while True:
            for task_name in running_task_info:
                if len(running_task_info[task_name]["depends"]) == 0 \
                        and self._m_runner_dict[task_name]["status"] == TaskStatus.WAITING:
                    ready_list.append(task_name)
                    self._m_runner_dict[task_name]["status"] = TaskStatus.READY

            ready_list.sort(key=lambda task_name: running_task_info[task_name]["run_id"])
            while len(ready_list) > 0 and (self._m_parallel_degree < 0 or \
                        len(self._m_running_tasks) < self._m_parallel_degree):
                task_name = ready_list.pop(0)
                self._m_running_tasks.add(task_name)
                self._m_runner_dict[task_name]["status"] = TaskStatus.RUNNING
                self._m_runner_dict[task_name]["runner"].start()

            finished_task_num = 0
            for task_name in self._m_running_tasks.copy():
                if self._m_runner_dict[task_name]["runner"].is_alive():
                    continue
                self._m_running_tasks.remove(task_name)

                exitcode = self._m_runner_dict[task_name]["runner"].exitcode
                if exitcode != 0:
                    self._m_runner_dict[task_name]["status"] = TaskStatus.FAILED
                    failed_task_num += 1
                    if not try_best:
                        self.terminate()
                        self.lists()
                        logging.critical("Task '{0}' failed, exit code {1}\n".format(
                                                                task_name, exitcode))
                        return exitcode
                else:
                    self._m_runner_dict[task_name]["status"] = TaskStatus.DONE
                    for depend_task_name in running_task_info[task_name]["reverse_depends"]:
                        running_task_info[depend_task_name]["depends"].remove(task_name)
                    running_task_info.pop(task_name)
                    finished_task_num +=1 

            verbose and disp.display()

            # All tasks finished successfully.
            if len(running_task_info) == 0 and len(self._m_running_tasks) == 0 \
                    and len(ready_list) == 0:
                break

            # Tasks which can be executed have all finished
            if try_best and len(self._m_running_tasks) == 0 \
                        and len(ready_list) == 0 \
                        and finished_task_num == 0 \
                        and failed_task_num > 0:
                verbose and disp.display(refresh=True)
                return 1
            time.sleep(0.1)

        verbose and disp.display(refresh=True)
        return 0

    def __kill_signal_handler(self, signum, stack):
        self.terminate()
        self._m_displayer_class(self._m_dependency_manager, self._m_runner_dict).display()
        logging.info("Receive signal %d, all running processes are killed.", signum)
        exit(1)

    def terminate(self):
        """Terminate all running processes."""
        for task_name in self._m_running_tasks.copy():
            self._m_runner_dict[task_name]["runner"].terminate()
            self._m_runner_dict[task_name]["status"] = TaskStatus.KILLED
            self._m_running_tasks.remove(task_name)

    def get_runner(self, task_name):
        """Get running instance of 'task_name'.

        Parameters
        ----------
        task_name: str
            Target task's name.

        Returns
        -------
        runner: TaskRunner, ProcRunner
            Reference runner of 'task_name'.

        """
        if task_name not in self._m_runner_dict:
            return None
        return self._m_runner_dict[task_name]["runner"]

    def _render_arguments(self, param):
        if isinstance(param, list):
            return list(map(self._render_arguments, param))

        if not isinstance(param, str):
            return param

        for match_str in re.findall(self._m_render_arg_pattern, param):
            match_str = match_str.strip()
            if match_str not in self._m_render_arguments:
                raise KeyError("missing value for render argument '{0}'".format(match_str))

        def __lookup_func(reg_match):
            return self._m_render_arguments[reg_match.group(1).strip()]
        return self._m_render_arg_pattern.sub(__lookup_func, param)


class MultiTaskConfig(object):
    def __init__(self, config, global_key="__global__"):
        self._m_global_key = global_key

        if isinstance(config, dict):
            self._m_config = copy.deepcopy(config)
        elif isinstance(config, str):
            with open(config, 'r') as fin:
                self._m_config = yaml.safe_load(fin)
        else:
            raise TypeError("Parameter 'config' must be a dict or a string for file name")

        self._m_manager = multiprocessing.Manager()
        self._m_share_config_lock = multiprocessing.Lock()
        self._m_share_config = self._m_manager.dict()
        self._m_share_config_hash = None
        self._render_config2share()

    @property
    def share_config(self):
        return self._m_share_config

    @property
    def share_config_lock(self):
        return self._m_share_config_lock

    def update(self):
        self._m_share_config_lock.acquire()
        try:
            new_share_config_hash = self.__share_config_hashcode()
            if new_share_config_hash == self._m_share_config_hash:
                return
            self._render_share2config()
            self._render_config2share()
        finally:
            self._m_share_config_lock.release()

    def dump_config(self, fname):
        fname = os.path.realpath(fname)
        save_path = os.path.dirname(fname)
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.update()
        with open(fname, 'w') as fout:
            yaml.safe_dump(self._m_share_config.copy(), fout, default_flow_style=False, indent=4)

    def save(self, fname):
        fname = os.path.realpath(fname)
        save_path = os.path.dirname(fname)
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.update()
        with open(fname, 'w') as fout:
            yaml.safe_dump(self._m_config, stream=fout, default_flow_style=False, indent=4)

    def _render_config2share(self):
        share_config = dict()
        for name, config in self._m_config.items():
            if name == self._m_global_key:
                continue
            share_config[name] = {}
            for key in ["input", "output"]:
                share_config[name][key] = {}
                for item, item_scope in config[key].items():
                    share_config[name][key][item] = self.__get_config_item(item_scope)
        self._m_share_config.update(share_config)
        self._m_share_config_hash = self.__share_config_hashcode()

    def _render_share2config(self):
        share_config = self._m_share_config.copy()
        for name, config in self._m_config.items():
            if name == self._m_global_key or name not in share_config:
                continue

            for item, item_scope in config["output"].items():
                if item not in share_config[name]["output"]:
                    logging.warning("Task '%s' did not output expected item [%s]", name, item)
                    continue
                self.__set_config_item(item_scope, share_config[name]["output"][item])
                share_config[name]["output"].pop(item)

            if len(share_config[name]["output"]) > 0:
                logging.warning("Task '%s' output unexpected values [%s]",
                        name, json.dumps(share_config[name]["output"]))

    def __get_config_item(self, scope):
        name_scope_list = scope.strip().split('.')
        if len(name_scope_list) == 0 or name_scope_list[0] != self._m_global_key:
            name_scope_list.insert(0, self._m_global_key)

        config = self._m_config
        for name_scope in name_scope_list:
            config = config[name_scope]
        return config

    def __set_config_item(self, scope, value):
        name_scope_list = scope.strip().split('.')
        if len(name_scope_list) == 0 or name_scope_list[0] != self._m_global_key:
            name_scope_list.insert(0, self._m_global_key)

        config = self._m_config
        for name_scope in name_scope_list[:-1]:
            config = config[name_scope]
        config[name_scope_list[-1]] = value

    def __share_config_hashcode(self):
        share_config_str = json.dumps(self._m_share_config.copy(), sort_keys=True)
        return hashlib.md5(share_config_str.encode("utf-8")).hexdigest()

