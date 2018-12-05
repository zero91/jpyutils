import sys
import unittest
import subprocess
import os

from jpyutils import runner
from jpyutils import utils

class TestTaskDependencyManager(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_add_dependency(self):
        dependency_manager = runner.multi_task_runner.TaskDependencyManager()

        dependency_manager.add_dependency("run_task1", "task1,task2")
        self.assertEqual(len(dependency_manager._m_task_id), 1)
        self.assertEqual(len(dependency_manager._m_task_id['run_task1']), 2)
        self.assertEqual(len(dependency_manager._m_dependency_info['run_task1']), 2)

        dependency_manager.add_dependency("run_task2", "run_task1")
        self.assertEqual(len(dependency_manager._m_task_id), 2)
        self.assertEqual(len(dependency_manager._m_task_id['run_task2']), 2)
        self.assertEqual(len(dependency_manager._m_dependency_info['run_task2']), 1)

    def test_get_dependency(self):
        dependency_manager = runner.multi_task_runner.TaskDependencyManager()
        dependency_manager.add_dependency("run_task1", "task1,task2")

        depend_tasks_set = dependency_manager.get_dependency("run_task1")
        self.assertEqual(len(depend_tasks_set), 2)
        self.assertTrue(isinstance(depend_tasks_set, set))
        self.assertTrue("task1" in depend_tasks_set)
        self.assertTrue("task2" in depend_tasks_set)

        with self.assertRaises(KeyError) as exception:
            dependency_manager.get_dependency("non_exist_task")

    def test_add_dependency(self):
        dependency_manager_1 = runner.multi_task_runner.TaskDependencyManager.from_data([
                ("run_task1", "task1,task2"),
                ("run_task2", "run_task1"),
        ])
        self.assertFalse(dependency_manager_1.is_topological())

        dependency_manager_2 = runner.multi_task_runner.TaskDependencyManager.from_data([
                ("task1", None),
                ("task2", None),
                ("run_task1", "task1,task2"),
                ("run_task2", "run_task1"),
        ])
        self.assertTrue(dependency_manager_2.is_topological())

    def test_get_tasks_info(self):
        dependency_manager = runner.multi_task_runner.TaskDependencyManager.from_data([
                ("task1", None),
                ("task2", None),
                ("run_task1", "task1,task2"),
                ("run_task2", "run_task1"),
        ])
        is_valid, task_info = dependency_manager.get_tasks_info()
        self.assertTrue(is_valid)
        self.assertEqual(len(task_info), 4)
        self.assertEqual(len(task_info['task1']), 4)

    def test_parse_tasks(self):
        dependency_manager = runner.multi_task_runner.TaskDependencyManager.from_data([
                ("task1", None),
                ("task2", None),
                ("run_task1", "task1,task2"),
                ("run_task2", "run_task1"),
        ])
        is_valid, tasks_info = dependency_manager.parse_tasks("1,2,4-8").get_tasks_info()
        self.assertTrue(is_valid)
        self.assertEqual(len(tasks_info), 2)


class TestMultiTaskRunner(unittest.TestCase):
    def setUp(self):
        utils.utilities.get_logger()

    def tearDown(self):
        pass

    def test_add(self):
        # add
        scheduler = runner.MultiTaskRunner(render_arguments={"mark": "jpyutils", "num": "2018"})
        scheduler.add(
            command = "ls -l",
            name = "test001",
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            shell=True
        )

        scheduler.add(
            command = "echo <%= mark %> + <%= num %>",
            name = "test002",
            depends="test001",
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            shell=True
        )

        scheduler.add(
            command = lambda s: sum(s),
            name = "add_sum",
            depends = "test002",
            args = (range(100000000),),
            stdout = None,
        )
        self.assertEqual(scheduler.run("0-2", try_best=True), 0)
        self.assertGreater(len(scheduler.get_task_runner("test002").stdout.read()), 0)
        # adds
        scheduler = runner.MultiTaskRunner(render_arguments={"mark": "jpyutils", "num": "2018"})

        scheduler.adds('Runner(name = "ls_<%= num %>", command = "ls",)')
        scheduler.adds(
            'Runner('
            '    name = "ls_<%=mark%>",'
            '    command = "ls",'
            '    depends = "ls_<%=num%>",'
            '    shell   = True,'
            ')'
        )
        self.assertEqual(scheduler.run(), 0)

        # addf
        scheduler = runner.MultiTaskRunner(render_arguments={"mark": "jpyutils", "num": "2018"})
        conf_path = os.path.dirname(os.path.realpath(__file__))
        scheduler.addf(os.path.join(conf_path, "multi_tasks.conf"))
        self.assertEqual(scheduler.run(), 0)

    def test_run(self):
        return
        conf_path = os.path.dirname(os.path.realpath(__file__))
        scheduler = runner.MultiTaskRunner(render_arguments={"mark": "jpyutils", "num": "2018"})
        scheduler.addf(os.path.join(conf_path, "multi_tasks.conf"))
        self.assertEqual(scheduler.run("2,3,5-7,10-11"), 0)

    def test_render_arguments(self):
        scheduler = runner.MultiTaskRunner(render_arguments={"mark": "jpyutils", "num": "2018"})
        self.assertEqual(scheduler._render_arguments("<%= mark %>"), "jpyutils")
        self.assertListEqual(scheduler._render_arguments(["<%= mark %>", "num = <%= num %>"]),
                             ["jpyutils", "num = 2018"])
        self.assertListEqual(scheduler._render_arguments([2018, "num = <%= num %>"]),
                             [2018, "num = 2018"])

if __name__ == '__main__':
    unittest.main()
