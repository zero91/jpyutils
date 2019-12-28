import lanfang
import os
import json


def parse_args():
  parser = lanfang.runner.ArgumentParser()
  parser.add_argument("--model", help="model type")
  parser.add_argument("--data", help="evaluate data")
  return parser.parse_args()


if __name__ == "__main__":
  args = parse_args()

  res = {
    "test_acc": 0.949
  }
  print(json.dumps(res))
