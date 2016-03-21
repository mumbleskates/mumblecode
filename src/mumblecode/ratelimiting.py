# coding=utf-8
from threading import Semaphore, Timer


class RateLimiter(object):
    def __init__(self, *limits):
        self.limits = [(Semaphore(limit), every) for limit, every in limits]

    def hit(self):
        for flag, duration in self.limits:
            flag.acquire()
            timer = Timer(duration, flag.release)
            timer.setDaemon(True)
            timer.start()
