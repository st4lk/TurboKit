# -*- coding: utf-8 -*-
from tornado import gen


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

    @gen.coroutine
    def __getitem__(self, index, *args, **kwargs):
        result = None
        if isinstance(index, slice):
            self.cursor = self.cursor[index]
            result = yield self.all()
        elif isinstance(index, (int, long)):
            self.cursor = self.cursor[index:index+1]
            result = yield self.all()
            result = result[0]
        else:
            raise TypeError(u"index {0} cannot be applied to Cursor "
                            u"instances".format(index))
        raise gen.Return(result)

    @gen.coroutine
    def all(self):
        response = yield self.cursor.to_list(None)
        raise gen.Return([self.cls(d) for d in response])
