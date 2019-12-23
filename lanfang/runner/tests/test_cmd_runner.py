import lanfang
import unittest
import subprocess


class TestCmdRunner(unittest.TestCase):
  def test_join(self):
    task = lanfang.runner.CmdRunner(
        target=["curl", "https://www.baidu.com"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    task.start()
    task.join()
    self.assertEqual(task.exitcode, 0)

  def test_context(self):
    context = lanfang.runner.RunnerContext()
    input_params = {"name": "Donald", "age": 18, "year": 2019}
    context.set_input("test_context", input_params)

    task = lanfang.runner.CmdRunner(
        target=["python", __file__],
        name="test_context",
        context=context,
        stdout=subprocess.PIPE)
    task.start()
    task.join()

    self.assertEqual(task.exitcode, 0)
    output = context.get_output(task.name)
    self.assertEqual(len(output), len(input_params) + 1)
    self.assertIn("params_num", output)
    self.assertEqual(len(output) - 1, output["params_num"])


if __name__ == "__main__":
  # The following code are used fo TestCmdRunner.test_context.
  import json
  args = vars(lanfang.runner.ArgumentParser().parse_args())
  args["params_num"] = len(args)
  print(json.dumps(args))
