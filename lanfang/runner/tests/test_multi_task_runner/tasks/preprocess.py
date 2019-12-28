import lanfang
import os
import json


def parse_args():
  parser = lanfang.runner.ArgumentParser()
  parser.add_argument("--vocab", help="Vocabulary")
  parser.add_argument("--train", help="train data")
  parser.add_argument("--dev", help="dev data")
  parser.add_argument("--test", help="test data")
  return parser.parse_args()


if __name__ == "__main__":
  args = parse_args()

  res = {
    "train": os.path.join("./test/features", "train.txt"),
    "dev": os.path.join("./test/features", "dev.txt"),
    "test": os.path.join("./test/features", "test.txt")
  }
  print(json.dumps(res))
