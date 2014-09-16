# -*- coding: utf-8 -*-
from tornado.concurrent import return_future
from functools import partial


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

    def skip(self, *args, **kwargs):
        self.cursor = self.cursor.skip(*args, **kwargs)
        return self

    def limit(self, *args, **kwargs):
        self.cursor = self.cursor.limit(*args, **kwargs)
        return self

    @classmethod
    @return_future
    def _get_first_element(cls, response, callback=None, **kwargs):
        if not response:
            raise IndexError("no such item for Cursor instance")
        callback(response[0])

    @return_future
    def __getitem__(self, index, *args, **kwargs):
        callback = kwargs['callback']
        if isinstance(index, slice):
            self.cursor = self.cursor[index]
            self.all(callback=callback)
        elif isinstance(index, (int, long)):
            self.cursor = self.cursor[index:index+1]
            self.all(callback=partial(self._get_first_element, callback=callback))
        else:
            raise TypeError(u"index {0} cannot be applied to Cursor "
                            u"instances".format(index))

    @return_future
    def all(self, callback):

        def handle_all_response(response, error):
            if error:
                raise error
            callback([self.cls(d) for d in response])

        self.cursor.to_list(None, callback=handle_all_response)
