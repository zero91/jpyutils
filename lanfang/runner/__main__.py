"""Main entry point"""
from lanfang import runner
import argparse
import json


import sys
if sys.argv[0].endswith("__main__.py"):
  import os.path
  # We change sys.argv[0] to make help message more useful
  # use executable without path, unquoted
  # (it's just a hint anyway)
  # (if you have spaces in your executable you get what you deserve!)
  executable = os.path.basename(sys.executable)
  sys.argv[0] = executable + " -m lanfang.runner"
  del os


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-d", "--start_dir", default=".",
    help="The directory of all the tasks.")

  parser.add_argument(
    "-l", "--lists", action="store_true",
    help="List all the registered tasks.")

  parser.add_argument(
    "-r", "--run", nargs='*',
    help="The tasks set to run.")

  parser.add_argument(
    "-v", "--verbose", action="store_true",
    help="Print verbose information when run all the tasks.")

  parser.add_argument(
    "--feed_values",
    help="The feed dict in json dumps to run the tasks.")

  parser.add_argument(
    "--print-params", action="store_true",
    help="Print the parameters of all the tasks")

  parser.add_argument(
    "--tasks", nargs="+", help="A subset of tasks to use.")

  return parser, parser.parse_args()


def main():
  parser, args = parse_args()

  if args.feed_values is None:
    feed_dict = {}
  else:
    feed_dict = json.loads(args.feed_values)

  runner.TaskLoader().load(args.start_dir)
  scheduler = runner.TaskRegister.spawn(feed_dict=feed_dict, subset=args.tasks)

  if args.print_params:
    print("--------------- Initial Parameters -------------")
    print(json.dumps(scheduler.run_params, indent=2, sort_keys=True))
    print("------------------------------------------------")

  if args.lists is True:
    scheduler.lists(verbose=True)
    exit(0)

  if args.run is None:
    scheduler.lists(verbose=True)
    parser.print_help()
    exit(0)

  if len(args.run) == 0:
    run_tasks = None
  else:
    run_tasks = args.run

  exit_code = scheduler.run(run_tasks, verbose=args.verbose)
  if args.print_params:
    print("---------------- Final Parameters --------------")
    print(json.dumps(scheduler.run_params, indent=2, sort_keys=True))
    print("------------------------------------------------")

  return exit_code


if __name__ == "__main__":
  exit(main())
