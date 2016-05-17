# coding=utf-8


class reentrant(object):
    def __init__(self, wrap):
        self.wrapped = wrap
        self.provided = None
        self.count = 0

    def __enter__(self):
        if self.count == 0:
            self.provided = self.wrapped.__enter__()
        self.count += 1
        return self.provided

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.count == 0:
            raise Exception("__exit__ called outside of all contexts")
        self.count -= 1
        # In reentrant context, we are returning None, which causes exceptions to bubble up normally
        # https://docs.python.org/3.5/reference/datamodel.html#object.__exit__
        if self.count == 0:
            self.provided = None
            return self.wrapped.__exit__(exc_type, exc_val, exc_tb)
