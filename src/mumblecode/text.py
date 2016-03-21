# coding=utf-8
from __future__ import unicode_literals


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

    >>> check_parens("()")
    set()
    >>> check_parens("((()())())")
    set()
    >>> check_parens("[<><>]", "[]<>")
    set()
    >>> check_parens("))asdf]", "[]")
    {(']', -1, 6)}
    >>> sorted(check_parens("{{{(", "{}()"))
    [('(', 1, None), ('{', 3, None)]
    >>> check_parens(check_parens.__doc__, "{}")
    {('{', 5, None)}
    >>> check_parens(check_parens.__doc__, "<>")
    {('>', -1, 960)}
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
