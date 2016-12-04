# coding=utf-8
from http.client import HTTPConnection, HTTPSConnection, _CS_IDLE
from collections import deque, namedtuple


RequestInfo = namedtuple('RequestInfo', 'method path body headers')


class Pipeline(object):
    def __init__(self, host, max_in_flight=5, https=True):
        conn_class = HTTPSConnection if https else HTTPConnection
        self._conn = conn_class(host)
        self._max_in_flight = max_in_flight
        self._in_flight = deque()

    def pipeline(self, requests):
        """
        Pipeline multiple HTTP requests to the server and handle all responses with callbacks.

        The implementation does NOT prepare an arbitrary number of pending requests;
        iterables of any length are viable here without wasting memory.

        :param requests: An enumerable of (RequestInfo, callback) tuples
        """
        for request, callback in requests:
            # send the request
            self._send_request(request, callback)

            # if we have enough requests in-flight, read a response now
            if len(self._in_flight) < self._max_in_flight:
                continue
            else:
                self._read_response()

        while self._in_flight:
            self._read_response()

    def _send_request(self, request, callback):
        self._conn._HTTPConnection__state = _CS_IDLE
        self._conn.request(*request)
        self._in_flight.append((
            request, callback,
            self._conn.response_class(self._conn.sock, method=self._conn._method)
        ))

    def _read_response(self):
        request, callback, response = self._in_flight.popleft()
        response.begin()
        callback(response)

        # connection is closing, we need to recreate the connection
        if response.will_close:
            self._conn.close()
            if not self._in_flight:
                return  # if we have nothing left to request, w're done
            # resend pending requests we never got responses to:
            # drain our old in-flight queue into the new one
            q, self._in_flight = self._in_flight, deque()
            while q:
                request, callback, _ = q.popleft()
                self._send_request(request, callback)
