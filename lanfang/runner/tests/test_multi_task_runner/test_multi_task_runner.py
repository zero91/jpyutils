import lanfang
import unittest
import subprocess
import os
import tempfile
import shutil


class TestRunnerInventory(unittest.TestCase):
  def test_add(self):
    inventory = lanfang.runner.multi_task_runner.RunnerInventory()
    inventory.add(
      name = "test001",
      target = "ls -l",
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
      shell=True)

    inventory.add(
      name = "test002",
      target = "echo 'this is test002'",
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
      shell=True)

    inventory.add(
      target = lambda s: print(sum(s)),
      name = "add_sum",
      args = (range(1000000),))

    task_info = inventory.get_info("test001")
    self.assertEqual(task_info["status"], lanfang.runner.RunnerStatus.WAITING)
    self.assertIsNone(task_info["start_time"])
    self.assertIsNone(task_info["elapsed_time"])
    self.assertTupleEqual(task_info["attempts"], (0, 1))


class TestMultiTaskRunner(unittest.TestCase):
  def test_not_share_params(self):
    scheduler = lanfang.runner.MultiTaskRunner()
    scheduler.add(
      target="ls -l && sleep 1",
      name="test001",
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      shell=True)

    scheduler.add(
      target="sleep 3 && cat xx",
      shell=True,
      name="failed_task")

    scheduler.add(
      target=["echo", "this is test002"],
      name="test002",
      depends="failed_task",
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE)

    scheduler.add(
      target=["sleep", "5"],
      name="sleep_task",
      depends="test001")

    scheduler.add(
      target=lambda s: print(sum(s)),
      name="add_sum",
      depends="test002",
      args=(range(10000000),))
    self.assertEqual(scheduler.run(try_best=True, verbose=True), 3)

  def _add_shared_tasks(self, scheduler):
    scheduler.add(
        name="fetch_vocab",
        target=["python", "./tasks/fetch_vocab.py"])

    scheduler.add(
        name="fetch_data",
        target=["python", "./tasks/fetch_data.py"])

    scheduler.add(
        name="preprocess",
        target=["python", "./tasks/preprocess.py"],
        depends=["fetch_vocab", "fetch_data"])

    scheduler.add(
        name="train_model",
        target=["python", "./tasks/train_model.py"],
        depends="preprocess")

    scheduler.add(
        name="evaluate",
        target=["python", "./tasks/evaluate.py"],
        depends=["train_model", "preprocess"])

    scheduler.add(
        name="analysis",
        target=["python", "./tasks/analysis.py"],
        depends=["train_model", "preprocess", "fetch_data"])

    return scheduler

  def test_share_params(self):
    current_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(current_path, "tasks/multi_task_config.jsonnet")
    log_path = tempfile.mkdtemp()
    checkpoint_path = os.path.join(log_path, "checkpoint")

    # basic usage
    scheduler = lanfang.runner.MultiTaskRunner(
        log_path=log_path,
        parallel_degree=1,
        config_file=config_file,
        config_kwargs={"fake_values": {'analysis_fork': ('train', 'dev')}})
    self._add_shared_tasks(scheduler)
 
    params = {
      'vocab_url': "http://fakeurl.com/vocabulary.tgz",
      'learning_rate': 3e-4,
      'model': "bilstm+crf",
      'data_url': "http://fakeurl.com/data.tgz",
      'analysis_fork': "dev",
      'locale': "zh_CN"
    }
    scheduler.set_params(params)

    self.assertEqual(len(scheduler.list()), 6)
    self.assertEqual(scheduler.run("0,1,4,5", verbose=True), 0)

    # save checkpoint
    checkpoint_info = scheduler.save(checkpoint_path, max_checkpoint_num=1)
    self.assertEqual(len(checkpoint_info), 3)
    self.assertTrue("runner_inventory" in checkpoint_info)
    self.assertTrue("context" in checkpoint_info)
    self.assertTrue("params" in checkpoint_info)
    self.assertDictEqual(checkpoint_info["params"], params)
    scheduler.close()

    # restore scheduler from previous saved checkpoint
    restore_scheduler = lanfang.runner.MultiTaskRunner(
        log_path=log_path,
        parallel_degree=1,
        config_file=config_file,
        config_kwargs={"fake_values": {'analysis_fork': ('train', 'dev')}})
    self._add_shared_tasks(restore_scheduler)

    restore_scheduler.restore(checkpoint_path)
    self.assertEqual(restore_scheduler.run(verbose=True), 0)
    self.assertEqual(len(restore_scheduler.list()), 6)
    restore_scheduler.save(checkpoint_path, max_checkpoint_num=2)
    restore_scheduler.close()
    shutil.rmtree(log_path)
