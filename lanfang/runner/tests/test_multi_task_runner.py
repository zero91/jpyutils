import lanfang
import sys
import unittest
import subprocess
import os


class TestMultiTaskParams(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_use(self):
    params_fname = "./conf/multi_task_config.json"
    params = lanfang.runner.multi_task_runner.MultiTaskParams(params_fname)
    params.share_params.update({"evaluate": {"output": {"train_acc": 0.91}}})
    params.dump(params_fname + "-no-debug.json", debug=False)
    params.dump(params_fname + "-debug.json", debug=True)


class TestMultiTaskRunner(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_add(self):
    # add
    scheduler = lanfang.runner.MultiTaskRunner()
    scheduler.add(
      target = "ls -l",
      name = "test001",
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
      shell=True)

    scheduler.add(
      target = "echo 'this is test002'",
      name = "test002",
      depends="test001",
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
      shell=True)

    scheduler.add(
      target = lambda s: print(sum(s)),
      name = "add_sum",
      depends = "test002",
      args = (range(1000000),)
    )
    self.assertEqual(scheduler.run("0-2", try_best=True), 0)

    # adds
    scheduler = lanfang.runner.MultiTaskRunner(
      render_arguments={"mark": "jpyutils", "num": "2018"})

    scheduler.adds('Runner(name = "ls_<%= num %>", target = "ls",)')
    scheduler.adds(
      'Runner('
      '  name = "ls_<%=mark%>",'
      '  target = "ls",'
      '  depends = "ls_<%=num%>",'
      '  shell   = True,'
      ')'
    )
    self.assertEqual(scheduler.run(), 0)

    # addf
    scheduler = lanfang.runner.MultiTaskRunner(
      render_arguments={"mark": "jpyutils", "num": "2018"}, log_path="./logs")
    conf_path = os.path.dirname(os.path.realpath(__file__))
    scheduler.addf(os.path.join(conf_path, "multi_tasks.conf"))
    scheduler.lists()
    self.assertEqual(scheduler.run(verbose=True), 0)

  def test_run(self):
    conf_path = os.path.dirname(os.path.realpath(__file__))
    scheduler = lanfang.runner.MultiTaskRunner(
      render_arguments={"mark": "jpyutils", "num": "2018"},
      parallel_degree=1,
      log_path="./xxx")
    scheduler.addf(os.path.join(conf_path, "multi_tasks.conf"))
    scheduler.lists()
    #self.assertEqual(scheduler.run("2,3,5-7"), 0)
    self.assertEqual(scheduler.run(verbose=True, try_best=True), 0)

  def test_lists(self):
    # addf
    scheduler = lanfang.runner.MultiTaskRunner(
      render_arguments={"mark": "jpyutils", "num": "2018"})
    conf_path = os.path.dirname(os.path.realpath(__file__))
    scheduler.addf(os.path.join(conf_path, "multi_tasks.conf"))
    print(scheduler.lists(display=False))

  def test_render_arguments(self):
    scheduler = lanfang.runner.MultiTaskRunner(
      render_arguments={"mark": "jpyutils", "num": "2018"})
    self.assertEqual(scheduler._render_arguments("<%= mark %>"), "jpyutils")
    self.assertListEqual(scheduler._render_arguments(["<%= mark %>", "num = <%= num %>"]),
               ["jpyutils", "num = 2018"])
    self.assertListEqual(scheduler._render_arguments([2018, "num = <%= num %>"]),
               [2018, "num = 2018"])


if __name__ == '__main__':
  unittest.main()

