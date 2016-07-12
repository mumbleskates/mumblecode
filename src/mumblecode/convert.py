# coding=utf-8


def int_to_bytes(i, order='little'):
    byte_length = (i.bit_length() + 7 + (i >= 0)) >> 3
    return i.to_bytes(byte_length, 'little', signed=True)


def bytes_to_int(b, order='little'):
    return int.from_bytes(b, order, signed=True)
