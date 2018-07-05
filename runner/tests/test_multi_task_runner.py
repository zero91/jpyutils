import sys
import unittest
import subprocess

from jpyutils import runner

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
        pass

    def tearDown(self):
        pass

    def _test_add_job(self):
        verbose = True
        #log = None
        #log = "log/"
        #log = subprocess.PIPE
        #print("hh")

        task = runner.MultiTaskRunner()
        task.add("sleep 1; ls -l", name = "test001", shell=True)
        task.add("sleep 2; ls -l /Users/zero91", name = "test002", depends="test001", shell=True)
        task.run(verbose=verbose)
        print("DONE")

    def _test_add_jobs_from_file():
        render_arguments = dict()
        render_arguments["LOCAL_ROOT"] = "../"
        render_arguments["HADOOP_BIN"] = "/home/zhangjian09/software/hadoop-client/hadoop/bin/hadoop"
        render_arguments["DATE"] = "2015-03-10"
        render_arguments["REF_DATE"] = "2015-03-18"
        render_arguments["HDFS_JOINED_LOG_DIR"] = "/app/ecom/fcr-opt/kr/zhangjian09/2015/data/join_kr_log"
        render_arguments["HDFS_ORIGIN_LOG_DIR"] = "/app/ecom/fcr-opt/kr/analytics"

        run_all = runner.MultiTaskRunner(log, render_arguments, parallel_degree=4)
        run_all.addf("conf/test.jobconf", "utf-8")
        run_all.lists()
        run_all.run(verbose=verbose)

        run_part = runner.MultiTaskRunner(log, render_arguments)
        run_part.addf("conf/test.jobconf", "utf-8")
        run_part.run("2,3,5-7,10-11", verbose=verbose)


if __name__ == '__main__':
    unittest.main()
