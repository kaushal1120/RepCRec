import argparse
import os

from manager_transaction import TransactionManager


def main(args):

    manager = TransactionManager()

    filepath = args.file
    with open(filepath, 'r') as f:
        for line in f:

            line = line.rstrip()
            is_comment = '//' in line
            if not is_comment:
                manager.getNextOperation(line)

    print('done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='input from file or dir')

    parser.add_argument('-f', '--file', type=str, help='path to test file')

    args = parser.parse_args()
    main(args)
