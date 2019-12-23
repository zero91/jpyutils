import lanfang
import unittest


class TestFuncProcessRunner(unittest.TestCase):
  def test_run(self):
    # Case 1: Test missing parameters
    def _proc_with_exception(a, b, name="test"):
      raise ValueError("Test Exception")

    runner_1 = lanfang.runner.FuncProcessRunner(
        target=_proc_with_exception, name="runer_1", retry=3, interval=0.1)
    runner_1.start()
    runner_1.join()
    self.assertEqual(runner_1.exitcode, 1)

    # Case 2: Test riase an exception
    runner_2 = lanfang.runner.FuncProcessRunner(
        target=_proc_with_exception,
        name="runner_2",
        args=(1, 2),
        retry=3,
        interval=0.1)
    runner_2.start()
    runner_2.join()
    self.assertEqual(runner_2.exitcode, 1)

    # Case 3: Normal
    runner_3 = lanfang.runner.FuncProcessRunner(
        target=sum, name="runner_3", args=(range(10),))
    runner_3.start()
    runner_3.join()
    self.assertEqual(runner_3.exitcode, 0)
    self.assertEqual(runner_3.output, 45)

  def test_return_value(self):
    def func(a, b):
      return "%d + %d = %d" % (a, b, a + b)

    runner = lanfang.runner.FuncProcessRunner(
        target=func, name="runner", args=(1, 3))
    runner.start()
    runner.join()
    self.assertEqual(runner.output, '1 + 3 = 4')

  def test_context(self):
    context = lanfang.runner.RunnerContext()
    input_params = {"name": "Donald", "age": 18, "money": 3000}
    context.set_input("test_context", input_params)

    def convert_to_dict(name, year, money=100):
      return {"name": name, "year": year, "money": money}

    runner = lanfang.runner.FuncProcessRunner(
        target=convert_to_dict, name="runner",
        args=("Donald", 2019),
        kwargs={"money": 50},
        context=context)
    runner.start()
    runner.join()
    self.assertDictEqual(runner.output, context.get_output(runner.name))
    self.assertEqual(runner.output["name"], "Donald")
    self.assertEqual(runner.output["year"], 2019)
    self.assertEqual(runner.output["money"], 50)


class TestFuncThreadRunner(unittest.TestCase):
  def test_run(self):
    # Case 1: Test missing parameters
    def _proc_with_exception(a, b, name="test"):
      raise ValueError("Test Exception")

    runner_1 = lanfang.runner.FuncThreadRunner(
        target=_proc_with_exception, name="runer_1", retry=3, interval=0.1)
    runner_1.start()
    runner_1.join()
    self.assertEqual(runner_1.exitcode, 1)

    # Case 2: Test riase an exception
    runner_2 = lanfang.runner.FuncThreadRunner(
        target=_proc_with_exception,
        name="runner_2",
        args=(1, 2),
        retry=3,
        interval=0.1)
    runner_2.start()
    runner_2.join()
    self.assertEqual(runner_2.exitcode, 1)

    # Case 3: Normal
    runner_3 = lanfang.runner.FuncThreadRunner(
        target=sum, name="runner_3", args=(range(10),))
    runner_3.start()
    runner_3.join()
    self.assertEqual(runner_3.exitcode, 0)
    self.assertEqual(runner_3.output, 45)

  def test_return_value(self):
    def func(a, b):
      return "%d + %d = %d" % (a, b, a + b)

    runner = lanfang.runner.FuncThreadRunner(
        target=func, name="runner", args=(1, 3))
    runner.start()
    runner.join()
    self.assertEqual(runner.output, '1 + 3 = 4')

  def test_context(self):
    context = lanfang.runner.RunnerContext()
    input_params = {"name": "Donald", "age": 18, "money": 3000}
    context.set_input("test_context", input_params)

    def convert_to_dict(name, year, money=100):
      return {"name": name, "year": year, "money": money}

    runner = lanfang.runner.FuncThreadRunner(
        target=convert_to_dict, name="runner",
        args=("Donald", 2019),
        kwargs={"money": 50},
        context=context)
    runner.start()
    runner.join()
    self.assertDictEqual(runner.output, context.get_output(runner.name))
    self.assertEqual(runner.output["name"], "Donald")
    self.assertEqual(runner.output["year"], 2019)
    self.assertEqual(runner.output["money"], 50)
