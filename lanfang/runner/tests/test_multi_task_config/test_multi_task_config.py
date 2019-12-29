import lanfang
import unittest
import os


class TestMultiTaskJsonnetConfig(unittest.TestCase):
  def setUp(self):
    config_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "multi_task_config.jsonnet")
    self._m_config = lanfang.runner.MultiTaskJsonnetConfig(
        config_file = config_file)

  def tearDown(self):
    pass

  def test_initialize(self):
    self.assertSetEqual(self._m_config.get_params(), {
        "locale", "data_url", "learning_rate"})

    with self.assertRaises(KeyError):
      self._m_config.get_config()

  def test_set_update_params(self):
    params = {
      "locale": "zh_CN",
      "data_url": "http://fakeurl.com",
      "learning_rate": 3e-4
    }
    # set params
    self._m_config.set_params(params)
    config = self._m_config.get_config()

    ## lcoal variables
    self.assertEqual(config["fetch_data"]["input"]["locale"], params["locale"])
    self.assertEqual(
        config["fetch_data"]["input"]["data_url"],
        params["data_url"])

    self.assertEqual(
        config["train"]["input"]["learning_rate"],
        params["learning_rate"])

    ## must be not replaced variables
    self.assertEqual(
        config["evaluate"]["input"]["template_should_not_be_replaced"],
        "<%= $.train.input.model  %>")

    self.assertEqual(
        config["evaluate"]["input"]["template_should_not_be_replaced_2"],
        "<%=  invalid_variable %>")

    ## reference variables
    self.assertEqual(
        config["evaluate"]["input"]["model"],
        "./model/checkpoint")
    self.assertEqual(config["train"]["input"]["train_data"], None)
    self.assertEqual(config["train"]["input"]["tag"], "zh_CN-bilstm")

    # update params
    self._m_config.update_params({"learning_rate": 0.01, "locale": "en_US"})
    config = self._m_config.get_config()

    self.assertEqual(config["fetch_data"]["input"]["locale"], "en_US")
    self.assertEqual(config["train"]["input"]["learning_rate"], 0.01)
    self.assertEqual(config["train"]["input"]["tag"], "en_US-bilstm")

  def test_update_output(self):
    params = {
      "locale": "zh_CN",
      "data_url": "http://fakeurl.com",
      "learning_rate": 3e-4
    }
    # set params
    self._m_config.set_params(params)
    config = self._m_config.get_config()
    self.assertEqual(config["train"]["output"]["train_acc"], None)

    # update output which have no task depends on this.
    self._m_config.update_output("train", {"train_acc": 0.8})
    config = self._m_config.get_config()
    self.assertEqual(config["train"]["output"]["train_acc"], 0.8)

    # update output which have no task depends on tthis.
    self.assertEqual(config["fetch_data"]["output"]["train"], None)
    self.assertEqual(config["fetch_data"]["output"]["dev"], None)
    self.assertEqual(config["fetch_data"]["output"]["test"], None)
    self.assertEqual(config["train"]["input"]["train_data"], None)
    self.assertEqual(config["train"]["input"]["dev_data"], None)
    self.assertEqual(config["evaluate"]["input"]["data"], None)

    self._m_config.update_output(
        "fetch_data", {"train": "./train", "dev": "./dev", "test": "./test"})
    config = self._m_config.get_config()

    self.assertEqual(config["train"]["output"]["train_acc"], 0.8)

    self.assertEqual(config["fetch_data"]["output"]["train"], "./train")
    self.assertEqual(config["fetch_data"]["output"]["dev"], "./dev")
    self.assertEqual(config["fetch_data"]["output"]["test"], "./test")
    self.assertEqual(config["train"]["input"]["train_data"], "./train")
    self.assertEqual(config["train"]["input"]["dev_data"], "./dev")
    self.assertEqual(config["evaluate"]["input"]["data"], "./test")
