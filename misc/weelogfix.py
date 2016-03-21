# coding=utf-8
from datetime import datetime
from functools import partial
from itertools import chain, groupby
from operator import itemgetter
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


def all_files(path):
    return chain.from_iterable(
        map(
            partial(os.path.join, contents[0]),
            contents[2]
        )
        for contents in os.walk(path)
    )


def filekey(root_path, regex=re.compile(
    r"^.*?[\.\\/]"
    r"(?:([_a-zA-Z#][\w#\-]*?)[\.\\/])?"
    r"([_a-zA-Z#][\w#\-]*?)[\.\\/]"
    r"(|[_a-zA-Z#][\w#\-]*?)\.(?:weechatlog|log)$"
)):
    def predicate(file_path):
        match = regex.match(file_path[len(root_path):])
        if match is None:
            return ()
        result = match.groups()
        if result[0] is None:
            result = result[1:]
        if result[-1] == "":
            result = result[:-1] + ("##global",)

        return result

    return predicate


def collate(iterables, *, reverse=False):
    """
    Accepts multiple iterables and yields items from them in a roughly sorted
    order.

    Each item from each iterator will appear once. If a unique item x is
    produced by an iterator before a unique item y from that same iterator, x
    will ALWAYS come before y in the resulting stream.

    Identical values are taken first from the earliest ordered iterator.

    >>> list(collate([[1, 2, 11, 12], [4, 5, 6]]))
    [1, 2, 4, 5, 6, 11, 12]
    >>> list(collate([
    ...     [5, 6, 7, 4, 5, 6],
    ...     [6, 1, 2, 3],
    ...     [8, 10, 12],
    ...     [8, 9, 11, 13]
    ... ]))
    [5, 6, 6, 1, 2, 3, 7, 4, 5, 6, 8, 8, 9, 10, 11, 12, 13]
    """
    first = max if reverse else min
    opened = []
    current = []
    # get the first items if they exist
    for feed in iterables:
        it = iter(feed)
        try:
            current.append(next(it))
        except StopIteration:
            continue
        else:
            opened.append(it)

    while opened:
        i, result = first(enumerate(current), key=itemgetter(1))
        # advance only the feed we just took the item from
        try:
            current[i] = next(opened[i])
        except StopIteration:
            # feed is finished, remove its entry in the lists
            del current[i]
            del opened[i]

        yield result


# this inspector is kinda dumb
# noinspection PyTypeChecker
def merge(iterables, *, reverse=False):
    """
    Merges multiple possibly incomplete iterators that may include selections
    or stretches of identical content into a single feed that includes every
    unique value at least once.

    Example usage: A sorted list of unique and comparable items is spread out
    among several smaller lists, which is each created by iterating over the
    original list and only taking certain items by an arbitrary standard. Many
    of the lists are very incomplete, but each item in the original list
    appears one or more times among the smaller lists. Passing all these lists
    into merge() will produce an iterator in the exact order and items of the
    original list, with none repeated.

    Likewise: merge(sorted_iterables) is equivalent to
    iter(sorted(set(chain.from_iterable(sorted_iterables))))

    >>> from random import sample, randint
    >>> original = list(range(1000))
    >>> small_lists = [[] for _ in range(10)]
    >>> for item in original:
    ...     for put_in in sample(range(10), randint(1, 5)):
    ...         small_lists[put_in].append(item)
    >>> original == list(merge(small_lists))
    True

    """
    first = max if reverse else min
    opened = []
    current = []
    for feed in iterables:
        it = iter(feed)
        try:
            current.append(next(it))
        except StopIteration:
            continue
        else:
            opened.append(it)

    while opened:
        result = first(current)
        # advance every feed that is going to provide an equal value
        # in reversed order so we can delete in-place while iterating
        for i in reversed(range(len(opened))):
            if current[i] == result:
                try:
                    current[i] = next(opened[i])
                except StopIteration:
                    del current[i]
                    del opened[i]

        yield result


def get_line_reader(path):
    print("     opening", path)
    extension = os.path.splitext(path)[-1] or os.path.split(path)[-1]
    return {
        ".weechatlog": WeechatLineFeed,
        ".log": HexchatLineFeed,
    }[extension](path)


class WeechatLineFeed(object):
    def __init__(self, path):
        self.fh = open(path, 'r', encoding='utf-8', newline='\n')
        self.line_time = datetime(1, 1, 1)
        self.line_pos = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.line_time is None:
            raise StopIteration
        try:
            line = next(self.fh)
        except StopIteration:
            self.close()
            raise

        self.line_pos += 1
        prev_line_time = self.line_time

        split = line.split("\t", 1)
        if len(split) == 1:
            print(split)
            return self.line_time, line
        else:
            timestamp, line_partial = split
            try:
                self.line_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                if prev_line_time > self.line_time:
                    print(
                        "warning: time jumped backwards in file\n"
                        "    file: {}\n"
                        "    line: {}\n"
                        "    from: {}\n"
                        "    to:   {}"
                        .format(
                            self.fh.name,
                            self.line_pos,
                            prev_line_time.isoformat(" "),
                            self.line_time.isoformat(" ")
                        ),
                        file=sys.stderr
                    )
            except ValueError:
                return self.line_time, line
            else:
                return self.line_time, line_partial

    def close(self):
        self.fh.close()
        self.line_time = None

    __del__ = close


class HexchatLineFeed(object):
    LINE_RE = re.compile(
        r"^\*\*\*\* (BEGIN|ENDING) LOGGING AT (.*)\n$|"
        r"^(?:"
        r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)"  # main date format
        r"|(\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d)"  # dotted date format
        r"|((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2} \d\d:\d\d:\d\d)"  # alternate date format
        r") (.*)$",  # body of log line
        re.DOTALL
    )

    def __init__(self, path):
        self.fh = open(path, 'r', encoding='utf-8')
        self.line_time = datetime(1, 1, 1)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.line_time is None:
                raise StopIteration
            try:
                line = next(self.fh)
            except StopIteration:
                self.close()
                raise

            m = HexchatLineFeed.LINE_RE.match(line)
            if m is None:
                return self.line_time, line

            begin_end_logging, logline_date, main_date, dotted_date, alt_date, line_body = m.groups()
            if logline_date:
                self.line_time = datetime.strptime(logline_date, "%a %b %d %H:%M:%S %Y")
                return self.line_time, "HEXCHAT: {} LOGGING\n".format(begin_end_logging)

            if not line_body.endswith("\n"):
                print("LINE BODY NOT TERMINATED", repr(line), repr(line_body))
            elif main_date:
                self.line_time = datetime.strptime(main_date, "%Y-%m-%d %H:%M:%S")
            elif dotted_date:
                self.line_time = datetime.strptime(dotted_date, "%Y.%m.%d %H:%M:%S")
            elif alt_date:
                self.line_time = datetime.strptime(
                    "{} {}".format(alt_date, self.line_time.year),  # alt-date does not include year
                    "%b %d %H:%M:%S %Y"
                )

            return self.line_time, line_body

    def close(self):
        self.fh.close()
        self.line_time = None

    __del__ = close


def output_file_handle(filehandle_dict, path, *args, **kwargs):
    if path in filehandle_dict:
        return filehandle_dict[path]
    else:
        folder = os.path.dirname(path)
        os.path.exists(folder) or os.makedirs(folder)
        result = filehandle_dict[path] = open(path, *args, **kwargs)
        print("    emitting", path)
        return result


def emit(date, body):
    if not body.endswith("\n"):
        body += "\n"
    return date.strftime("%Y-%m-%d %H:%M:%S") + "\t" + body


def main(outputpath, *inputpaths):
    inputpaths = [os.path.realpath(p) for p in inputpaths]
    outputpath = os.path.realpath(outputpath)

    file_lists = []
    all_keys = set()
    for i, in_path in enumerate(inputpaths):
        this_filekey = filekey(in_path)
        file_lists.append({})
        for fp in all_files(in_path):
            key = this_filekey(fp)
            file_lists[i].setdefault(key, []).append(fp)
            all_keys.add(key)
    for key in all_keys:
        if key is ():  # ignore the group of files that don't match our pattern
            continue
        print(".".join(key))

        # collating before merging instead of just merging everything isn't strictly necessary;
        # however, there are some weird edge cases with multi-file logging i wanna hit without
        # losing data. I may remove this later and just merge() everything.
        sources = []
        for input_files in file_lists:
            if key in input_files:
                sources.append(collate(get_line_reader(f) for f in input_files[key]))

        opened = {}
        try:
            for line in merge(sources):
                date, body = line
                destination = os.path.join(
                    outputpath,
                    str(date.year), "{:02}".format(date.month),
                    ".".join(key) + ".weechatlog"
                )
                output_file_handle(opened, destination, 'a', encoding='utf-8', newline='\n').write(emit(date, body))
        finally:
            for f in opened.values():
                f.close()
    print("Done!")


if __name__ == "__main__":
    main(*sys.argv[1:])
