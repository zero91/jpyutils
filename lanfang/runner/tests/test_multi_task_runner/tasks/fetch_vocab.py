import lanfang
import urllib
import os
import json
import random
import time


def parse_args():
  parser = lanfang.runner.ArgumentParser()
  parser.add_argument("--locale", help="Locale of data")
  parser.add_argument("--data_url", help="Url of data")
  return parser.parse_args()


if __name__ == "__main__":
  time.sleep(random.random() * 5)

  args = parse_args()
  server_path = urllib.parse.urlparse(args.data_url).path[1:]
  res = {"vocab_file": os.path.join("./test", args.locale, server_path)}
  print(json.dumps(res))
