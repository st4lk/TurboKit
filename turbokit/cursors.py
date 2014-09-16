# -*- coding: utf-8 -*-
import functools
from tornado.concurrent import return_future


class AsyncManagerCursor(object):

    def __init__(self, cls, cursor, batch=20):
        self.cursor = cursor
        self.cls = cls
        self.batch = batch

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

        return_list = []

        def handle_all_response(response, error, return_list):
            if error:
                raise error
            else:
                if response:
                    return_list += [self.cls(document)
                                    for document in response]
                    self.cursor.to_list(self.batch,
                        callback=functools.partial(handle_all_response,
                        return_list=return_list))
                else:
                    callback(return_list)

        self.cursor.to_list(self.batch, callback=functools.partial(
            handle_all_response, return_list=return_list))
