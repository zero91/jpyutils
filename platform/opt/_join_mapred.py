"""Internal joining tools.
"""
# Author: Donald Cheung <jianzhang9102@gmail.com>
import sys
import re
import os
import hashlib
import operator
import argparse

def map_input_match(pattern):
    current_file = os.getenv('map_input_file')
    if current_file is None:
        sys.stderr.write("current file got by env `map_input_file' is None\n")
        exit(1)

    if re.match(pattern, current_file):
        return True
    else:
        sys.stderr.write('current_file={0}, pattern={1}\n'.format(current_file, pattern.pattern))
        return False


class Mapper(object):
    def __init__(self, args):
        self.__method = args.method
        self.__left_pattern = re.compile(args.left_pattern)
        self.__left_key_list = map(int, args.left_key.split(','))
        self.__left_value_list = list()
        for value in args.left_value.split(','):
            idx, default = value.split(':')
            self.__left_value_list.append((int(idx), default))

        self.__right_pattern = re.compile(args.right_pattern)
        self.__right_key_list   = map(int, args.right_key.split(','))
        self.__right_value_list = list()
        for value in args.right_value.split(','):
            idx, default = value.split(':')
            self.__right_value_list.append((int(idx), default))

    def run(self):
        if self.__method == "left" or self.__method == "inner":
            left_tag = 1
            right_tag = 0
        elif self.__method == "right":
            left_tag = 0
            right_tag = 1
        else:
            sys.stderr.write("Unsupported join method [{0}]\n".format(self.__method))
            exit(1)
        
        if map_input_match(self.__left_pattern):
            is_left_file = True
        elif map_input_match(self.__right_pattern):
            is_left_file = False
        else:
            sys.stderr.write("unknown input file")
            exit(1)
        
        for line in sys.stdin:
            fields = line[:-1].split('\t')
            try:
                if is_left_file is True:
                    left_key_str = '\t'.join(map(lambda k: fields[k], self.__left_key_list))
                    left_key = hashlib.md5(left_key_str).hexdigest()
                    left_value = '\t'.join(map(lambda k: fields[k[0]], self.__left_value_list))
                    sys.stdout.write("{0}\t{1}\t{2}\n".format(left_key, left_tag, left_value))

                else:
                    right_key_str = '\t'.join(map(lambda k: fields[k], self.__right_key_list))
                    right_key = hashlib.md5(right_key_str).hexdigest()
                    right_value = '\t'.join(map(lambda k: fields[k[0]], self.__right_value_list))
                    sys.stdout.write("{0}\t{1}\t{2}\n".format(right_key, right_tag, right_value))

            except Exception as e:
                sys.stderr.write("Exception [{0}]\n".format(e))


class Reducer(object):
    def __init__(self, method, left_value, right_value):
        self.__method = method 
        self.__left_value_list = list()
        for value in left_value.split(','):
            idx, default = value.split(':')
            self.__left_value_list.append((int(idx), default))

        self.__right_value_list = list()
        for value in right_value.split(','):
            idx, default = value.split(':')
            self.__right_value_list.append((int(idx), default))

    def run(self):
        if self.__method == "left":
            self.__left()

        elif self.__method == "right":
            self.__right()

        elif self.__method == "inner":
            self.__inner()

        else:
            sys.stderr.write("Unsupported joining method [{0}]\n".format(self.__method))
            exit(1)

    def __left(self):
        right_key = None
        right_value_list = list()
        right_default_value = "\t".join(map(operator.itemgetter(1), self.__right_value_list))

        for line in sys.stdin:
            fields = line[:-1].split('\t')
            if fields[1] == '0':
                if right_key != fields[0]:
                    right_key = fields[0]
                    right_value_list = list()
                right_value_list.append('\t'.join(fields[2:]))

            else:
                left_value = '\t'.join(fields[2:])
                if fields[0] == right_key:
                    for right_value in right_value_list:
                        sys.stdout.write("{0}\t{1}\n".format(left_value, right_value))
                else:
                    sys.stdout.write("{0}\t{1}\n".format(left_value, right_default_value))

    def __right(self):
        left_key = None
        left_value_list = list()
        left_default_value = "\t".join(map(operator.itemgetter(1), self.__left_value_list))

        for line in sys.stdin:
            fields = line[:-1].split('\t')
            if fields[1] == '0':
                if left_key != fields[0]:
                    left_key = fields[0]
                    left_value_list = list()
                left_value_list.append('\t'.join(fields[2:]))

            else:
                right_value = '\t'.join(fields[2:])
                if fields[0] == left_key:
                    for left_value in left_value_list:
                        sys.stdout.write("{0}\t{1}\n".format(left_value, right_value))
                else:
                    sys.stdout.write("{0}\t{1}\n".format(left_default_value, right_value))

    def __inner(self):
        right_key = None
        right_value_list = list()
        for line in sys.stdin:
            fields = line[:-1].split('\t')
            if fields[1] == '0':
                if right_key != fields[0]:
                    right_key = fields[0]
                    right_value_list = list()
                right_value_list.append('\t'.join(fields[2:]))

            else:
                left_value = '\t'.join(fields[2:])
                if fields[0] != right_key:
                    continue
                for right_value in right_value_list:
                    sys.stdout.write("{0}\t{1}\n".format(left_value, right_value))


def parse_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-e", "--execute", required=True, help="Operations: map/reduce")
    arg_parser.add_argument("-m", "--method", help="Join method: left/right/inner")
    arg_parser.add_argument("--left_pattern", help="Left input path pattern.")
    arg_parser.add_argument("--left_key", help = "Left input data keys.")
    arg_parser.add_argument("--left_value", help = "Left input data values.")
    arg_parser.add_argument("--right_pattern", help="Right input path pattern.")
    arg_parser.add_argument("--right_key", help = "Right input data keys.")
    arg_parser.add_argument("--right_value", help = "Right input data values.")
    return arg_parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    if args.execute == 'map':
        Mapper(args).run()

    elif args.execute == 'reduce':
        Reducer(args.method, args.left_value, args.right_value).run()

    else:
        sys.stderr.write("[ERROR] Unsupported operation [{0}]\n".format(args.execute))
        exit(1)
