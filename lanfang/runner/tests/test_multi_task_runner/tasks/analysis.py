import lanfang
import os
import json


def parse_args():
  parser = lanfang.runner.ArgumentParser()
  parser.add_argument("--model", help="model type")
  parser.add_argument("--data", help="evaluate data")
  parser.add_argument("--original_data", help="original data")
  return parser.parse_args()


if __name__ == "__main__":
  args = parse_args()

  res = {
    "report": os.path.join("./test", "analysis", os.path.basename(args.model))
  }
  print(json.dumps(res))
