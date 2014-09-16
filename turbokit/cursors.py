# -*- coding: utf-8 -*-
from tornado.concurrent import return_future


class AsyncManagerCursor(object):

    def __init__(self, cls, cursor):
        self.cursor = cursor
        self.cls = cls

    @property
    def fetch_next(self):
        return self.cursor.fetch_next

    def next_object(self):
        result = self.cursor.next_object()
        return self.cls(result)

    def sort(self, *args, **kwargs):
        self.cursor = self.cursor.sort(*args, **kwargs)
        return self

    @return_future
    def all(self, callback):

        def handle_all_response(response, error):
            if error:
                raise error
            callback([self.cls(d) for d in response])

        self.cursor.to_list(None, callback=handle_all_response)
