# coding=utf-8
from math import log2, ceil


# valid chars for a url path component: a-z A-Z 0-9 .-_~!$&'()*+,;=:@
# For the default set here (base 72) we have excluded $'();:@
radix_alphabet = ''.join(sorted(
    "0123456789"
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    ".-_~!&*+,="
))
radix = len(radix_alphabet)
radix_lookup = {ch: i for i, ch in enumerate(radix_alphabet)}
length_limit = ceil(128 / log2(radix))  # don't decode numbers much over 128 bits


# TODO: add radix alphabet as parameter
# TODO: fix format so length conveys m ore information (e.g. 0 and 00 and 000 are different with decimal alphabet)


def int_to_natural(i):
    i *= 2
    if i < 0:
        i = -i - 1
    return i


def natural_to_int(n):
    sign = n & 1
    n >>= 1
    return -n - 1 if sign else n


def natural_to_url(n):
    """Accepts an int and returns a url-compatible string representing it"""
    # map from signed int to positive int
    url = ""
    while n:
        n, digit = divmod(n, radix)
        url += radix_alphabet[digit]

    return url or radix_alphabet[0]


def url_to_natural(url):
    """Accepts a string and extracts the int it represents in this radix encoding"""
    if not url or len(url) > length_limit:
        return None

    n = 0
    try:
        for ch in reversed(url):
            n = n * radix + radix_lookup[ch]
    except KeyError:
        return None

    return n


def int_to_bytes(i, order='little'):
    byte_length = (i.bit_length() + 7 + (i >= 0)) >> 3
    return i.to_bytes(byte_length, order, signed=True)


def bytes_to_int(b, order='little'):
    return int.from_bytes(b, order, signed=True)
