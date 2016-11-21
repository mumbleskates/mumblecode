# coding=utf-8
from __future__ import unicode_literals

import re


def check_parens(string, pairs="()"):
    """
    Check a string for non-matching braces or other character pairs and return
    a list of failures, or an empty set if everything is OK.

    `if check_parens(string, brackets):` is a good way to find bad brackets in
    a string.

    Pairs should be a string of paired braces, such as "[]" or "{}[]<>()".
    If `pairs` is excluded, only checks parentheses. Checking pairs of quotes
    (where the opening and closing character is the same) is not supported;
    every character in `pairs` should be unique.

    The return value is a set of tuples of (character, depth, position). The
    function immediately returns with the first dangling closing character it
    finds, with the position it was found at in the string and a depth of -1. If
    the end of the string is reached and there are still dangling contexts, the
    set may have multiple values with varying positive depth for each opening
    character, but the position will be None.

    >>> check_parens("()") == set()
    True
    >>> check_parens("((()())())") == set()
    True
    >>> check_parens("[<><>]", "[]<>") == set()
    True
    >>> check_parens("))asdf]", "[]") == {(']', -1, 6)}
    True
    >>> check_parens("{{{(", "{}()") == {('(', 1, None), ('{', 3, None)}
    True
    >>> check_parens(check_parens.__doc__, "{}") == {('{', 5, None)}
    True
    >>> check_parens(check_parens.__doc__, "<>") == {('>', -1, 960)}
    True
    """
    begins = {}
    ends = {}
    counters = []
    it = iter(pairs)
    for begin, end in zip(it, it):
        begins[begin] = len(counters)
        ends[end] = len(counters)
        counters.append(0)

    for position, ch in enumerate(string):
        if ch in begins:
            counters[begins[ch]] += 1
        elif ch in ends:
            index = ends[ch]
            counters[index] -= 1
            if counters[index] < 0:
                return {(ch, -1, position)}

    return {
        (begin, counters[ct_idx], None)
        for begin, ct_idx in begins.items() if counters[ct_idx]
    }


def multireplace(string, replacements):
    """
    Given a string and a replacement map, it returns the replaced string.
    :param str string: string to execute replacements on
    :param dict replacements: replacement dictionary {value to find: value to replace}
    :rtype: str
    """
    # from https://gist.github.com/bgusach/a967e0587d6e01e889fd1d776c5f3729
    # Place longer ones first to keep shorter substrings from matching where the longer ones should take place
    # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against the string 'hey abc', it should produce
    # 'hey ABC' and not 'hey ABc'
    substrs = sorted(replacements, key=len, reverse=True)
    # Create a big OR regex that matches any of the substrings to replace
    regexp = re.compile('|'.join(map(re.escape, substrs)))
    # For each match, look up the new string in the replacements
    return regexp.sub(lambda match: replacements[match.group(0)], string)
