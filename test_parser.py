#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from __future__ import print_function
import os, shutil, re
import sys
import argparse

def main(arguments):
    parser = argparse.ArgumentParser(
        description="Apply some rules (default or stored) to manage rolling backup file on days,weeks,monthes ans years",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-n', '--invariant', help="Name of the file backuped inside all current backup file's name", nargs=1)
    parser.add_argument('-b', '--files_path', help="Path to directory containing the files backuped", nargs=1)
    parser.add_argument('-r', '--config_rules', help="Name of the file containing rules", type=argparse.FileType('r'), nargs=1)
    parser.add_argument('-d', '--defaultrules', help="Display default rules", action='store_true')
    parser.add_argument('-t', '--testrules', help="Dry run : test rules ans display result on console", action='store_true')
    parser.add_argument('-a', '--archdir', help="set archive path for non deletion option", nargs=1, default='./archived')
    parser.add_argument('-l', '--logfile', help="log file",
                        default=sys.stdout, type=argparse.FileType('w'), nargs=1)
    print(arguments)
    args = parser.parse_args(arguments)
    print(args)

if __name__ == '__main__':
    sys.exit(main("-t -r ./check_bckp_file.conf -l log.log -n E12_LABSED2019-04-prod.zip -b ./test_dir".split(" ")))
else:
    sys.exit(main(sys.argv[1:]))
