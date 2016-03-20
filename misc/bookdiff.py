# coding=utf-8
from functools import partial
from itertools import chain
import os


"""
Diff between a spreadsheet of ebooks and actual ebook files in a folder
"""


bookshelf_path = r"D:\Docs\Documents\Dropbox\Bookshelf"

SPREADSHEET = """\
"""


def all_files(path):
    return chain.from_iterable(
        map(
            partial(os.path.join, contents[0]),
            contents[2]
        )
        for contents in os.walk(path)
    )


def display_books(books):
    for book in sorted(books):
        print("    {}, {} - {}".format(*book))


def book_files():
    filepaths = list(filter(lambda x: x.endswith(".epub"), all_files(bookshelf_path)))
    for file in filepaths:
        author, bookfile = file.split(os.sep)[-2:]
        try:
            [firstname, lastname] = author.replace(".", "").rsplit(None, 1)
        except ValueError:
            pass
        else:
            bookname = os.path.splitext(bookfile)[0]
            yield lastname.lower(), firstname.lower(), bookname.replace("'", "").lower()


# noinspection PyTypeChecker
def books_spreadsheet():
    for entry in SPREADSHEET.splitlines():
        row = entry.split('\t')
        full_row = row + [None] * (5 - len(row))
        author, title, series_num, priority, status = full_row
        if not author:
            continue
        lastname, firstname = author.replace(".", "").split(', ')
        if status == "acquired":
            yield lastname.lower(), firstname.lower(), title.replace("'", "").lower()


if __name__ == '__main__':
    archived = set(book_files())
    listed = set(books_spreadsheet())
    archive_missing = listed - archived
    list_missing = archived - listed
    print("Archive is missing:")
    display_books(archive_missing)
    print("Listing is missing:")
    display_books(list_missing)
