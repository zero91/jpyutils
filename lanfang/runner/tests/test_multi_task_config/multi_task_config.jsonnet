// Jsonnet format file for multiple task configuration
// used by lanfang/runner/test/test_multi_task_config.py
{
  "fetch_data": {
    "input": {
      "locale": <%= locale %>,
      "data_url": <%= data_url %>
    },
    "output": {
      "train": null,
      "dev": null,
      "test": null
    }
  },
  "train": {
    "input": {
      "model": "bilstm",
      "train_data": <%= $.fetch_data.output.train %>,
      "dev_data": <%= $.fetch_data.output.dev %>,
      "learning_rate": <%= learning_rate %>,
      "tag": <%= locale %> + "-" + self.model
    },
    "output": {
      "model_path": "./model/checkpoint",
      "train_acc": null,
      "dev_acc": null
    }
  },
  "evaluate": {
    "input": {
      "model": <%= $.train.output.model_path %>,
      "data": <%= $.fetch_data.output.test %>,
      "template_should_not_be_replaced": "<%= $.train.input.model  %>",
      "template_should_not_be_replaced_2": "<%=  invalid_variable %>"
    },
    "output": {
      "test_acc": null
    }
  }
}
