# coding=utf-8


def ignore_exception(ignore=Exception, default_val=None):
    """ Decorator for ignoring exception from a function
    e.g.   @ignore_exception(DivideByZero)
    e.g.2. ignore_exception(DivideByZero)(Divide)(2/0)
    """

    def dec(function):
        def _dec(*args, **kwargs):
            # noinspection PyBroadException
            try:
                return function(*args, **kwargs)
            except ignore:
                return default_val

        return _dec

    return dec