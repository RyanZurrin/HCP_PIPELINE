import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('initial', help='the initial substring to be replaced')
parser.add_argument('final', help='the final substring to replace the initial substring')
parser.add_argument('file_path', help='the file path of the files to be changed')
args = parser.parse_args()


def change_file_name_substrings(initial, final, file_path):
    """Change the parts of the file names that match the initial substring withing the file name to the final substring.

    :param initial: the initial substring to be replaced
    :param final: the final substring to replace the initial substring
    :param file_path: the file path of the files to be changed
    :return: None
    """
    print('changing file name substrings')
    print('path: ', file_path)
    # get a list of the files in the file_path
    file_list = file_path.glob('*')
    # loop through the files in the file_path
    for file in file_list:
        # get the file name
        file_name = file.name
        print('file_name: ', file_name)
        # if the file name contains the initial substring
        if initial in file_name:
            # get the new file name
            new_file_name = file_name.replace(initial, final)
            print('new_file_name: ', new_file_name)
            # change the file name
            file.rename(file.parent / new_file_name)


# if someone runs this script from the command line then run the main function
if __name__ == '__main__':
    change_file_name_substrings(args.initial, args.final, Path(args.file_path))
