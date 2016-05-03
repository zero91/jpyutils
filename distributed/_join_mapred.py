# coding: gbk
import sys
import re
import os
import hashlib
from argparse import ArgumentParser

def map_input_match(pattern):
    current_file = os.getenv('map_input_file')
    if current_file != None:
        if re.match(pattern, current_file):
            return True
        else:
            sys.stderr.write('current_file=[%s], pattern=[%s]\n' % (current_file, pattern.pattern))
            return False
    else:
        sys.stderr.write('current file got by env map_input_file is None\n')
        exit(-1)

class Mapper:
    def __init__(self, args):
        self.__method = args.method

        self.__left_input_pattern = re.compile(args.left_input_pattern)
        self.__left_key_list = map(lambda k: int(k), args.left_key_list.split(','))
        self.__left_value_list  = map(lambda k: int(k), args.left_value_list.split(','))
        self.__left_fields_num  = args.left_fields_num

        self.__right_input_pattern = re.compile(args.right_input_pattern)
        self.__right_key_list   = map(lambda k: int(k), args.right_key_list.split(','))
        self.__right_value_list = map(lambda k: int(k), args.right_value_list.split(','))
        self.__right_fields_num = args.right_fields_num

    def run(self):
        if self.__method == "left" or self.__method == "inner":
            left_tag = 1
            right_tag = 0
        elif self.__method == "right":
            left_tag = 0
            right_tag = 1
        else:
            sys.stderr.write("Unsupported join method [%s]\n" % self.__method)
            exit(-1)
        
        for line in sys.stdin:
            cells = line.strip().split('\t')
            if map_input_match(self.__left_input_pattern):
                if len(cells) != self.__left_fields_num:
                    continue
                left_key_str = '\t'.join(map(lambda k: cells[k], self.__left_key_list))
                left_key = hashlib.md5(left_key_str).hexdigest()

                left_value = '\t'.join(map(lambda k: cells[k], self.__left_value_list))
                sys.stdout.write("%s\t%d\t%s\n" % (left_key, left_tag, left_value))

            elif map_input_match(self.__right_input_pattern):
                if len(cells) != self.__right_fields_num:
                    continue
                right_key_str = '\t'.join(map(lambda k: cells[k], self.__right_key_list))
                right_key = hashlib.md5(right_key_str).hexdigest()

                right_value = '\t'.join(map(lambda k: cells[k], self.__right_value_list))
                sys.stdout.write("%s\t%d\t%s\n" % (right_key, right_tag, right_value))
            else:
                sys.stderr.write("Can not match map_input_file to left or right input.\n")
                exit(-1)

class Reducer:
    def __init__(self, method, left_value_num, right_value_num):
        self.__method = method 
        self.__left_value_num = left_value_num
        self.__right_value_num = right_value_num

        self.__left_null_value = "\t".join(["-1"] * self.__left_value_num)
        self.__right_null_value = "\t".join(["-1"] * self.__right_value_num)

    def run(self):
        if self.__method == "left":
            self.__left()

        elif self.__method == "right":
            self.__right()

        elif self.__method == "inner":
            self.__inner()
        else:
            sys.stderr.write("Unsupported join method [%s]\n" % self.__method)
            exit(-1)

    def __left(self):
        right_key = ""
        right_value_list = list()
        for line in sys.stdin:
            line = line.strip()
            if line == "":
                continue
            cells = line.split('\t')
            if cells[1] == '0':
                if right_key != cells[0]:
                    right_key = cells[0]
                    right_value_list = list()
                right_value_list.append('\t'.join(cells[2:]))
            elif cells[1] == '1':
                left_value = '\t'.join(cells[2:])
                if cells[0] == right_key:
                    for right_value in right_value_list:
                        sys.stdout.write("%s\t%s\n" % (left_value, right_value))
                else:
                    sys.stdout.write("%s\t%s\n" % (left_value, self.__right_null_value))
            else:
                sys.stderr.write("ERROR FORMAT tag [%s]\n" % cells[1])

    def __right(self):
        left_key = ""
        left_value_list = list()
        for line in sys.stdin:
            line = line.strip()
            if line == "":
                continue
            cells = line.split('\t')
            if cells[1] == '0':
                if left_key != cells[0]:
                    left_key = cells[0]
                    left_value_list = list()
                left_value_list.append('\t'.join(cells[2:]))
            elif cells[1] == '1':
                right_value = '\t'.join(cells[2:])
                if cells[0] == left_key:
                    for left_value in left_value_list:
                        sys.stdout.write("%s\t%s\n" % (left_value, right_value))
                else:
                    sys.stdout.write("%s\t%s\n" % (self.__left_null_value, right_value))
            else:
                sys.stderr.write("ERROR FORMAT tag [%s]\n" % cells[1])

    def __inner(self):
        right_key = ""
        right_value_list = list()
        for line in sys.stdin:
            line = line.strip()
            if line == "":
                continue
            cells = line.split('\t')
            if cells[1] == '0':
                if right_key != cells[0]:
                    right_key = cells[0]
                    right_value_list = list()
                right_value_list.append('\t'.join(cells[2:]))
            elif cells[1] == '1':
                left_value = '\t'.join(cells[2:])
                if cells[0] == right_key:
                    for right_value in right_value_list:
                        sys.stdout.write("%s\t%s\n" % (left_value, right_value))
            else:
                sys.stderr.write("ERROR FORMAT tag [%s]\n" % cells[1])

def parse_args():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-e", "--execute", required = True,
                                    help = "Operation to be executed, mapper or reducer")
    arg_parser.add_argument("--left_input_pattern",
                                    help = "Pattern which left input path must be satisfied")
    arg_parser.add_argument("--left_key_list", help = "Left input's key list")
    arg_parser.add_argument("--left_value_list", help = "Left input's value list")
    arg_parser.add_argument("--left_fields_num", type = int, help = "Left input's fields num")

    arg_parser.add_argument("--right_input_pattern",
                                    help = "Pattern which right input path must be satisfied")
    arg_parser.add_argument("--right_key_list", help = "Right input's key list")
    arg_parser.add_argument("--right_value_list", help = "Right input's value list")
    arg_parser.add_argument("--right_fields_num", type = int, help = "Right input's fields num")

    arg_parser.add_argument("-m", "--method", help = "left, right, inner")
    arg_parser.add_argument("--left_value_num", type = int,
                                    help = "Left input's value column num")
    arg_parser.add_argument("--right_value_num", type = int,
                                    help = "Right input's value column num")
    return arg_parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    if args.execute == 'mapper':
        mapper = Mapper(args)
        mapper.run()
    elif args.execute == 'reducer':
        reducer = Reducer(args.method, args.left_value_num, args.right_value_num)
        reducer.run()
    else:
        sys.stderr.write("[ERROR] Unsupported operation [%s]\n" % args.execute)
        exit(-1)
