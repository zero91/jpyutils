"""Tools for running a simple task, or running a bunch of tasks which may contain dependency
relations. It can executing tasks efficiently in parallel if the dependency relationship
between them can be expressed as a topology graph.
"""

# Author: Donald Cheung <jianzhang9102@gmail.com>

from .. import utils

import os
import sys
import subprocess
import threading
import time
import datetime
import signal
import re
import copy

_TASK_DISABLED = 0
_TASK_WAITING  = 1
_TASK_READY    = 2
_TASK_RUNNING  = 3
_TASK_DONE     = 4
_TASK_FAILED   = 5
_TASK_KILLED   = 6
_TASK_BFAILED  = 7
_TASK_AFAILED  = 8

_task_status_info = {
    _TASK_DISABLED : ("Disabled", "white"),
    _TASK_WAITING  : ("Waiting",  "yellow"),
    _TASK_READY    : ("Ready",    "blue"),
    _TASK_RUNNING  : ("Running",  "cyan"),
    _TASK_DONE     : ("Done",     "green"),
    _TASK_FAILED   : ("Failed",   "red"),
    _TASK_KILLED   : ("Killed",   "purple"),
    _TASK_BFAILED  : ("BFailed",  "red"),
    _TASK_AFAILED  : ("AFailed",  "red")
}

class TaskRunner(threading.Thread):
    """Maintaining a task in a python thread.

    It maintains a task, which may be used to executing a simple task.

    Parameters
    ----------
    command: string
        The command which need to be executed.

        The shell argument (which defaults to True) specifies whether to use the shell as the
        program to execute. If shell is True, it is recommended to pass args as a string rather
        than as a sequence.

    name: string
        The name of the task. Best using naming method of programming languages,
        for it may be used to create log files on disk.

    log: string
        The file/directory which used to save logs. Default to be subprocess.PIPE,
        which save logs internally, which may cause dead LOCK when output data is large.
        If log is a directory, log will be write to filename specified by self.name end
        up with "stdout"/"stderr".

    shell: boolean
        Default to be true.

    check_before: string
        The check command to be executed before this task.

    check_after: string
        The check command to be executed after this task.

    retry: integer
        Try executing the command retry times until succeed, otherwise failed.

    Notes
    -----
    TaskRunner.stdout
        If the log is set to be subprocess.PIPE, this attribute is a file object that provides
        stdout to the current running process. Otherwise, it is None.

    TaskRunner.stdout
        If the log is set to be subprocess.PIPE, this attribute is a file object that provides
        stderr from the current running process. Otherwise, it is None.

    TaskRunner.returncode
        Attribute which specify the exit code of this task.
    """
    def __init__(self, command, name = None,
                                log = subprocess.PIPE,
                                shell = True,
                                check_before = None,
                                check_after = None,
                                retry = 1):
        threading.Thread.__init__(self, name=name)
        self.__shell = shell
        self.__retry = retry

        self.__run_info = {
            'before' : {
                'cmd' : check_before,
                'started' : False,
                'process' : None,
                'returncode' : None,
                'failed_code' : _TASK_BFAILED
            }, 
            'main' : {
                'cmd' : command,
                'started' : False,
                'process' : None,
                'returncode' : None,
                'failed_code' : _TASK_FAILED
            },
            'after' : {
                'cmd' : check_after,
                'started' : False,
                'process' : None,
                'returncode' : None,
                'failed_code' : _TASK_AFAILED
            }
        }
        self.__current_process = None
        self.__retry_id = 0
        self.__status = _TASK_READY
        self.__start_time = None
        self.__elapse_time = datetime.timedelta(0, 0, 0)
        self.__stdout = log
        self.__stderr = log

        self.returncode = None
        self.stderr = None
        self.stdout = None

    def run(self):
        """Start running this job.
        """
        if self.__start_time is not None: # already run once.
            return 

        self.__init_log()
        self.__start_time = datetime.datetime.now()
        for try_time in xrange(1, self.__retry + 1):
            self.__retry_id = try_time
            if type(self.__stderr) == file:
                self.__stderr.write("\nTry %d time(s)\n" % self.__retry_id)
                self.__stderr.flush()

            for phase, info in self.__run_info.iteritems():
                info['started'] = False
                info['process'] = None
                info['returncode'] = None

            self.__status = _TASK_RUNNING
            for phase in ("before", "main", "after"):
                while self.__run_command(phase) is None:
                    time.sleep(0.1)

                if self.__run_info[phase]['returncode'] != 0:
                    if type(self.__stderr) is file:
                        self.__stderr.write("\nPhase [%s] failed for task [%s]\n" % \
                                                    (phase, self.name))
                        self.__stderr.flush()
                    self.returncode = self.__run_info[phase]['returncode']
                    self.__status = self.__run_info[phase]['failed_code']
                    break

            if self.returncode is not None and self.returncode != 0:
                continue

            self.returncode = 0
            self.__status = _TASK_DONE
            self.__elapse_time = datetime.datetime.now() - self.__start_time
            return

    def suicide(self):
        """Suicide this task.
        """
        if self.__current_process != None and self.__current_process.poll() == None:
            os.kill(self.__current_process.pid, signal.SIGTERM)
            self.__status = _TASK_KILLED
        return True
    
    def get_status(self):
        if self.__status == _TASK_RUNNING:
            self.__elapse_time = datetime.datetime.now() - self.__start_time
    
        elapse_time = self.__elapse_time.total_seconds() + \
                            self.__elapse_time.microseconds / 1000000
        info_dict = { 'status' : self.__status,
                      'elapse_time' : elapse_time,
                      'start_time' : self.__start_time,
                      'returncode' : self.returncode,
                      'try_info' : "%s/%s" % (self.__retry_id, self.__retry) }
        return info_dict

    def __init_log(self):
        if self.__stdout is not None and self.__stdout != subprocess.PIPE:
            log_path = self.__stdout
            if os.path.isdir(log_path):
                self.__stdout = open("%s/%s.stdout" % (log_path, self.name), "a+")
                self.__stderr = open("%s/%s.stderr" % (log_path, self.name), "a+")

            elif os.path.isfile(log_path):
                self.__stdout = open(log_path, 'a+')
                self.__stdrr = self.__stdout

            elif os.path.isdir(os.path.dirname(os.path.abspath(log_path))):
                self.__stdout = open("%s.stdout" % (log_path), 'a+')
                self.__stderr = open("%s.stderr" % (log_path), 'a+')

    def __run_command(self, phase):
        if self.__run_info[phase]['cmd'] is None:
            self.__run_info[phase]['returncode'] = 0

        elif not self.__run_info[phase]['started']:
            self.__run_info[phase]['process'] = subprocess.Popen(self.__run_info[phase]['cmd'],
                                                                 stdin=None,
                                                                 stdout=self.__stdout,
                                                                 stderr=self.__stderr,
                                                                 shell=self.__shell,
                                                                 close_fds=True)
            self.__current_process = self.__run_info[phase]['process']
            self.__run_info[phase]['returncode'] = self.__run_info[phase]['process'].poll()
            self.__run_info[phase]['started'] = True
            self.stderr = self.__current_process.stderr
            self.stdout = self.__current_process.stdout
        else:
            self.__run_info[phase]['returncode'] = self.__run_info[phase]['process'].poll()
        return self.__run_info[phase]['returncode']

class MultiTaskRunner:
    """A task manager class.

    It maintains a set of tasks, and run the tasks according to their topological relations.

    Parameters
    ----------
    log: string
        The file/directory which used to save logs. Default to be subprocess.PIPE,
        which save logs in each task internally, which may cause dead LOCK when
        output data is large.  If log is a directory, log will be write to filename
        specified by each task's name end up with "stdout"/"stderr".

    render_arguments: dict
        Dict which used to replace tasks' parameter for its true value.

    parallel_degree: integer
        Parallel degree of this task. At most parallel_degree of the tasks will be run
        simultaneously.

    retry: integer
        Try executing each task retry times until succeed.
    """
    def __init__(self, log = subprocess.PIPE,
                       render_arguments = {},
                       parallel_degree = sys.maxint,
                       retry = 1):
        self.__log = log
        self.__render_arguments = render_arguments
        self.__parallel_degree = parallel_degree
        self.__retry = retry

        self.__task_id = None
        self.__task_depends_list = list()
        self.__task_runner = dict()
        self.__task_running = set()
        self.__running_tasks = None
        self.__running_tasks_depends = dict()

        self.__started = False

    def add(self, command, name=None,
                           shell=True,
                           check_before=None,
                           check_after=None,
                           depends=None):
        """Add a new task.

        Parameters
        ----------
        command: string
            The command which need to be executed.

            The shell argument (which defaults to True) specifies whether to use the shell as the
            program to execute. If shell is True, it is recommended to pass args as a string
            rather than as a sequence.

        name: string
            The name of the task. Best using naming method of programming languages,
            for it may be used to create log files on disk.

        check_before: string
            The check command to be executed before the task.

        check_after: string
            The check command to be executed after the task.

        depends: string
            A string which is the concatenation of the names of all the tasks which must be
            executed ahead of this task. Separated by a single comma(',').

        shell: boolean
            Default to be true.
        """
        if name in self.__task_runner:
            sys.stderr.write("Task [%s] is already exists!\n" % name)
            return False
        runner = TaskRunner(command, name = name,
                                     log = self.__log,
                                     shell = shell,
                                     check_before = check_before,
                                     check_after = check_after,
                                     retry = self.__retry)

        self.__task_runner[runner.name] = runner
        if depends is None:
            depends_task_set = set()
        else:
            depends_task_set = set(map(lambda item: item.strip(), depends.split(',')))
        self.__task_depends_list.append((runner.name, depends_task_set))
        return True

    def adds(self, tasks_str, encoding="utf-8"):
        """Add tasks from a string.

        Parameters
        ----------
        tasks_str : string
            The string of the tasks.
        """
        tasks_str = tasks_str.decode(encoding)
        render_arg_pattern = re.compile(r"\<\%=(.*?)\%\>")
        all_match_str = re.findall(render_arg_pattern, tasks_str)
        for match_str in all_match_str:
            if match_str not in self.__render_arguments:
                sys.stderr.write("[ERROR] Missing value for render argument [%s].\n" % match_str)
                continue

        def __lookup_func(self, reg_match):
            return self.__render_arguments[reg_match.group(1).strip()]
        tasks_str = render_arg_pattern.sub(__lookup_func, tasks_str)
        exec(tasks_str, {}, {'TaskRunner': self.add})
        return True

    def addf(self, tasks_fname, encoding="utf-8"):
        """Add tasks from a file

        Parameters
        ----------
        tasks_fname : string
            The file's name of which contains tasks.
        """
        return self.adds(open(tasks_fname, 'r').read(), encoding)

    def lists(self, update=False):
        """List all jobs in topological_order with job id.
        """
        if not self.__valid_topological(update):
            return False
        _MultiTaskProgressDisplay(self.__task_id,
                                 dict(self.__task_depends_list),
                                 self.__task_runner).display()

    def run(self, tasks=None, verbose=False):
        """Running all jobs of this task.

        Parameters
        ----------
        tasks : string
            The tasks which needed to be executed, sepecified by topological ids,
            format like "1-3,5,8,9-10".

        Notes
        -----
            Should only be executed once.
        """
        if self.__started:
            sys.stderr.write("Task should be executed only once")
            return False

        if not self.__valid_topological():
            sys.stderr.write("Task dependency graph is not topological!\n")
            return False
        self.__started = True

        signal.signal(signal.SIGINT, self.__kill_handler)
        signal.signal(signal.SIGTERM, self.__kill_handler)

        self.__running_tasks = self.__parse_running_tasks(tasks)
        depends_dict = dict(map(lambda (t, d): (t, d.intersection(self.__running_tasks)), \
                                filter(lambda (t, d): t in self.__running_tasks, \
                                        copy.deepcopy(self.__task_depends_list))))

        reverse_depends_dict = dict()
        for name, depends_set in depends_dict.iteritems():
            for depend in depends_set:
                reverse_depends_dict.setdefault(depend, set())
                reverse_depends_dict[depend].add(name)

        self.__running_tasks_depends = depends_dict

        progress = _MultiTaskProgressDisplay(self.__task_id,
                                            depends_dict,
                                            self.__task_runner,
                                            self.__running_tasks)
        verbose and progress.display()

        ready_list = list()
        while True:
            for name, depends_set in depends_dict.iteritems():
                if len(depends_set) == 0:
                    ready_list.append(name)

            for name in ready_list:
                depends_dict.pop(name, None)

            ready_list = sorted(ready_list, key = lambda t: self.__task_id[t])
            while len(self.__task_running) < self.__parallel_degree and len(ready_list) > 0:
                task_name = ready_list.pop(0)
                self.__task_running.add(task_name)
                self.__task_runner[task_name].setDaemon(True)
                self.__task_runner[task_name].start()

            for name in self.__task_running.copy():
                if self.__task_runner[name].is_alive():
                    continue
                self.__task_running.remove(name)

                if self.__task_runner[name].returncode != 0:
                    self.__kill_all_processes()
                    progress.display()
                    sys.stderr.write("Task [%s] failed, return code [%d]\n" % (
                                            self.__task_runner[name].name,
                                            self.__task_runner[name].returncode))
                    return False

                if name in reverse_depends_dict:
                    for depends in reverse_depends_dict[name]:
                        depends_dict[depends].remove(name)

            verbose and progress.display()
            if len(depends_dict) == 0 and len(self.__task_running) == 0 and len(ready_list) == 0:
                break
            time.sleep(0.1)

        progress.display()
        return True

    def __valid_topological(self, update=False):
        if update is not True and self.__task_id is not None:
            return self.__task_id is not False

        depends_dict = dict(copy.deepcopy(self.__task_depends_list))
        reverse_depends_dict = dict()
        for name, depends_set in depends_dict.iteritems():
            for depend in depends_set:
                reverse_depends_dict.setdefault(depend, set())
                reverse_depends_dict[depend].add(name)
        task_initial_order = dict(map(lambda (i, d) : (d[0], i), \
                                        enumerate(self.__task_depends_list)))

        tot_task_num = len(depends_dict)
        self.__task_id = dict()
        while len(self.__task_id) < tot_task_num:
            ready_list = list()
            for name, depends_set in depends_dict.iteritems():
                if len(depends_set) == 0:
                    ready_list.append(name)
            if len(ready_list) == 0:
                self.__task_id = False
                return False

            for name in sorted(ready_list, key=lambda name: task_initial_order[name]):
                self.__task_id[name] = len(self.__task_id) + 1
                if name in reverse_depends_dict:
                    for depends in reverse_depends_dict[name]:
                        depends_dict[depends].remove(name)
                depends_dict.pop(name)
        return True
    
    def __parse_running_tasks(self, tasks):
        if tasks == None:
            return set(map(lambda item: item[0], self.__task_depends_list))

        running_set = set()
        for task_segs in tasks.split(','):
            task_segs = task_segs.strip()
            if task_segs == '':
                continue
            tasks = map(lambda task: int(task) if (task.strip() != '') else sys.maxint, \
                        task_segs.split('-'))
            running_set |= set(range(min(tasks), min(len(self.__task_id), max(tasks)) + 1))
        return filter(lambda t: self.__task_id[t] in running_set, self.__task_id.keys())

    def __kill_handler(self, signum, stack):
        self.__kill_all_processes()
        _MultiTaskProgressDisplay(self.__task_id,
                                 self.__running_tasks_depends,
                                 self.__task_runner,
                                 self.__running_tasks).display()
        sys.stderr.write("\nReceive signal %d, all running processes are killed.\n" % signum)
        exit(-1)

    def __kill_all_processes(self):
        for name in self.__task_running.copy():
            self.__task_runner[name].suicide()
            self.__task_running.remove(name)

class _MultiTaskProgressDisplay():
    def __init__(self, task_id, depends_dict, task_runner, running_tasks = None):
        self.__task_id = task_id
        self.__depends_dict = depends_dict
        self.__last_depends_dict = copy.deepcopy(depends_dict)
        self.__task_runner = task_runner
        self.__running_tasks = running_tasks

        self.__task_id_len = len(str(max(self.__task_id.values()))) + 3
        self.__task_name_left = self.__task_id_len + 1

        self.__pos = dict([ ('id', len(str(max(self.__task_id.values()))) + 3),
                            ('task_name', max(map(lambda t: len(t), self.__task_id.keys()))),
                            ('status', 8),
                            ('start_time', 19), # format: YYYY-mm-dd HH:MM:SS
                            ('elapse_time', 12),
                            ('try_info', 5) ])

        self.__tot_len = sum(self.__pos.values())
        self.__last_status = dict()
        self.__task_num = len(self.__task_id)

    def display(self):
        for (task_name, task_id) in sorted(self.__task_id.iteritems(), key=lambda m: m[1]):
            self.__display_task(task_name)
        self.__last_depends_dict = copy.deepcopy(self.__depends_dict)

    def __display_task(self, task_name):
        task_status = self.__task_runner[task_name].get_status()
        if task_name in self.__last_status and task_status == self.__last_status[task_name] \
                                           and self.__depends_dict == self.__last_depends_dict:
            return
        update = (task_name in self.__last_status)

        self.__last_status[task_name] = copy.deepcopy(task_status)
        move = (self.__task_num - self.__task_id[task_name] + 1) * 2

        status_str = ("[%d]. " % self.__task_id[task_name]).ljust(self.__pos['id'])
        status_str += task_name.ljust(self.__pos['task_name'])

        highlight = True
        status = task_status['status']
        if self.__running_tasks is not None and task_name not in self.__running_tasks:
            status = _TASK_DISABLED
            highlight = False
        elif task_name in self.__depends_dict and len(self.__depends_dict[task_name]) > 0:
            status = _TASK_WAITING
        status_info = _task_status_info[status]

        status_str += " | "
        status_str += utils.shell.tint(status_info[0].ljust(self.__pos['status']),
                                        font_color=status_info[1], highlight=highlight)

        if task_status['start_time'] is not None:
            status_str += " | "
            status_str += task_status['start_time'].strftime("%Y.%m.%d %H:%M:%S")
            status_str += " | "
            status_str += str(task_status['elapse_time']).ljust(self.__pos['elapse_time'])
            status_str += " | "
            status_str += task_status['try_info'].ljust(self.__pos['try_info'])

        if task_name in self.__depends_dict:
            status_str += " | "
            status_str += ",".join(self.__depends_dict[task_name])

        if update:
            sys.stderr.write("\033[%dA\33[K%s\033[%dB\n" % (move, status_str, move - 1))
        else:
            row_separator_len = sum(self.__pos.values()) + 3 * len(self.__pos.values()) - 5
            sys.stderr.write("%s\n" % ("-" * row_separator_len))
            sys.stderr.write("%s\n" % (status_str))
            if len(self.__last_status) == len(self.__task_id):
                sys.stderr.write("%s\n" % ("-" * row_separator_len))
