import lanfang
import os
import json


def parse_args():
  parser = lanfang.runner.ArgumentParser()
  parser.add_argument("--model", help="model type")
  parser.add_argument("--train_data", help="train data")
  parser.add_argument("--dev_data", help="dev data")
  parser.add_argument("--learning_rate", type=float, default=0.1)
  parser.add_argument("--tag", help="experiment tag")
  return parser.parse_args()


if __name__ == "__main__":
  args = parse_args()

  res = {
    "model_path": os.path.join("./test/model", args.tag, args.model),
    "train_acc": 0.967,
    "dev_acc": 0.942
  }
  print(json.dumps(res))
