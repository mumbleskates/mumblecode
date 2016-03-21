# coding=utf-8
from functools import partial
from itertools import chain, groupby
import os
import re
import sys


"""
I had some indecision/confusion about what name and path format I wanted to
use with my weechat irc logs. This resulted in a number of different files in
different folders, by various standards (for instance,
irc/freenode/#python.weechatlog, irc.freenode.#python.weechatlog,
2016/02/irc.freenode.#python.weechatlog...) and over different and mixed time
periods.

I settled on logger.file.mask = "%Y/%m/$plugin.$name.weechatlog", which means
year/mo/info.weechatlog in the path. This is a simple library to gather up the
confused files, organize them by common destinations, then interleave the
lines of files with a common destination, in the correct order by date, while
still maintaining the line order of lines that originated from any given file.
"""


def filekey(root_path, regex=re.compile(
    r"^.*?[\.\\/]"
    r"(?:([_a-zA-Z#][\w#\-]*?)[\.\\/])?"
    r"([_a-zA-Z#][\w#\-]*?)[\.\\/]"
    r"([_a-zA-Z#][\w#\-]*?)\.weechatlog$"
)):
    def predicate(file_path):
        match = regex.match(file_path[len(root_path):])
        if match is None:
            return ()
        result = match.groups()
        if result[0] is None:
            return result[1:]
        else:
            return result

    return predicate


def reorder_lines(file_paths):
    opened = []
    try:
        # open files and get their first lines if they exist
        current = []
        for path in file_paths:
            fh = open(path, 'r', encoding='utf-8', newline='\n')
            print("    opened", path)
            try:
                current.append(next(fh))
            except StopIteration:
                continue
            else:
                opened.append(fh)

        while opened:
            result = min(current)
            idx_next = current.index(result)
            # load the next line of the file we just took a line from
            try:
                current[idx_next] = next(opened[idx_next])
            except StopIteration:
                # file is finished, remove its entry in the lists
                del current[idx_next]
                opened[idx_next].close()
                del opened[idx_next]

            yield result
    finally:
        for f in opened:
            f.close()


def file_handle(filehandle_dict, path, *args, **kwargs):
    if path in filehandle_dict:
        return filehandle_dict[path]
    else:
        folder = os.path.dirname(path)
        os.path.exists(folder) or os.makedirs(folder)
        result = filehandle_dict[path] = open(path, *args, **kwargs)
        print("    emitting", path)
        return result


def all_files(path):
    return chain.from_iterable(
        map(
            partial(os.path.join, contents[0]),
            contents[2]
        )
        for contents in os.walk(path)
    )


def main(inputpath, outputpath):
    date_regex = re.compile(r"^(\d\d\d\d)-(\d\d)-")
    inputpath = os.path.realpath(inputpath)
    outputpath = os.path.realpath(outputpath)

    allfiles = sorted(all_files(inputpath), key=filekey(inputpath))
    for info, files in groupby(allfiles, filekey(inputpath)):
        if info is ():  # ignore the group of files that don't match our pattern
            continue
        print(".".join(info))
        opened = {}
        try:
            for line in reorder_lines(files):
                year, month = date_regex.match(line).groups()
                destination = os.path.join(outputpath, year, month, ".".join(info) + ".weechatlog")
                file_handle(opened, destination, 'a', encoding='utf-8', newline='\n').write(line)
        finally:
            for f in opened.values():
                f.close()
    print("Done!")


if __name__ == "__main__":
    main(*sys.argv[1:])
