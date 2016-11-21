# coding=utf-8
import os
from shutil import copyfileobj
from sys import argv
from tqdm import tqdm


def main(src_folder, src_encoding, dest_folder, dest_encoding):
    src_folder = os.path.abspath(src_folder)
    dest_folder = os.path.abspath(dest_folder)

    for root, dirs, files in os.walk(src_folder):
        to_root = dest_folder + root[len(src_folder):]
        if not os.path.exists(to_root):
            os.makedirs(to_root)

        print("Copying {} to {}".format(repr(root.encode('utf-8')), repr(to_root.encode('utf-8'))))
        for filename in tqdm(files, unit="files"):
            frompath = os.path.join(root, filename)
            topath = os.path.join(to_root, filename)
            try:
                with open(frompath, 'r', encoding=src_encoding) as source, \
                        open(topath, 'w', encoding=dest_encoding) as destination:
                    copyfileobj(source, destination)
            except UnicodeDecodeError as ex:
                print("Unicode error on file {}".format(repr(frompath.encode('utf-8'))))
                print(ex)


if __name__ == '__main__':
    main(*argv[1:])  # todo: use argparse
