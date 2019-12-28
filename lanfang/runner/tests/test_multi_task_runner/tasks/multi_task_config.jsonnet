// Jsonnet format file for multiple task configuration
// used by lanfang/runner/test/multi_task_runner
{
  "fetch_vocab": {
    "input": {
      "locale": <%= locale %>,
      "data_url": <%= vocab_url %>
    },
    "output": {
      "vocab_file": null
    }
  },
  "fetch_data": {
    "input": {
      "locale": <%= locale %>,
      "data_url": <%= data_url %>
    },
    "output": {
      "train": null,
      "dev": "fetch_data.output.dev",
      "test": null
    }
  },
  "preprocess": {
    "input": {
      "vocab": <%= $.fetch_vocab.output.vocab_file %>,
      "train": <%= $.fetch_data.output.train %>,
      "dev": <%= $.fetch_data.output.dev %>,
      "test": <%= $.fetch_data.output.test %>
    },
    "output": {
      "train": null,
      "dev": "preprocess.output.dev",
      "test": null
    }
  },
  "train_model": {
    "input": {
      "model": <%= model %>,
      "train_data": <%= $.preprocess.output.train %>,
      "dev_data": <%= $.preprocess.output.dev %>,
      "learning_rate": <%= learning_rate %>,
      "tag": <%= locale %> + "-" + self.model
    },
    "output": {
      "model_path": "model_checkpoint",
      "train_acc": null,
      "dev_acc": null
    }
  },
  "evaluate": {
    "input": {
      "model": <%= $.train_model.output.model_path %>,
      "data": <%= $.preprocess.output.test %>
    },
    "output": {
      "test_acc": null
    }
  },
  "analysis": {
    "input": {
      "model": <%= $.train_model.output.model_path %>,
      "data": {
        "train": $.preprocess.output.train,
        "dev": $.preprocess.output.dev,
        "test": $.preprocess.output.test}[<%= analysis_fork %>],
      "original_data": {
        "train": $.fetch_data.output.train,
        "dev": $.fetch_data.output.dev,
        "test": $.fetch_data.output.test}[<%= analysis_fork %>],
      "vocab": <%= $.fetch_vocab.output.vocab_file %>
    },
    "output": {
      "report": null
    }
  }
}
