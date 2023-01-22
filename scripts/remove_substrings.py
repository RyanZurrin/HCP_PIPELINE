# read a file line by line and on each line look for the first = sign and remove
# the substring after the = sign up to the end of the line but not including the
# end of the line

import argparse
from pathlib import Path

INITIAL = '='

parser = argparse.ArgumentParser()
parser.add_argument('file',
                    help='the file to be changed')
# add optional filepath argument
parser.add_argument('-i', '--initial',
                    help='the initial char to start removing substrings on each line',
                    default=INITIAL)

args = parser.parse_args()


def remove_substrings(file, initial_char):
    """Remove parts of each line that start with initial_char and end with final_char
    not including the final_char as part of the removed substring.
    if a line does not have the initial_char then the line is not changed.

    :param file: the file path of the file to be changed
    :param initial_char: the initial character of the substring to be removed

    :return: None
    """
    print('removing substrings from file')
    print('path: ', file)
    # get a list of the lines in the file
    with open(file, 'r') as f:
        lines = f.readlines()

    # var to save the new lines
    new_lines = []
    # loop through the lines in the file
    for line in lines:
        # if the line contains the initial substring
        if initial_char in line:
            # get the new line
            print('initial line: ', line)
            new_line = line[:line.find(initial_char)]
            print('new_line: ', new_line)
            # add the new line to the new lines list
            new_lines.append(new_line)
        else:
            # add the line to the new lines list
            new_lines.append(line)

    # write the new lines to the file
    with open(file, 'w') as f:
        # loop through the new lines and write them to the file
        for line in new_lines:
            # add each line to the file with a new line character
            f.write(line + '\n')


if __name__ == '__main__':
    remove_substrings(args.file, args.initial)