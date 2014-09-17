# -*- coding: utf-8 -*-
import logging
from tornado import gen
from .customtypes import ModelReferenceType

l = logging.getLogger(__name__)


class AsyncManagerCursor(object):

    def __init__(self, cls, cursor, db=None, prefetch_related=None):
        self.cursor = cursor
        self.cls = cls
        self.db = db
        self._prefetch_related = set(prefetch_related) if prefetch_related else set()

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

    def prefetch_related(self, *args):
        self._prefetch_related |= set(args)
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
        results = [self.cls(d) for d in response]
        prefetch_ids = []
        if results:
            parent_model = results[0]
            for pr in self._prefetch_related:
                field = parent_model._fields.get(pr, None)
                if field:
                    if isinstance(field, ModelReferenceType):
                        prefetch_ids.append(
                            (field, pr, map(lambda m: getattr(m, pr), results)))
                    else:
                        l.warning("prefetch_related field must be instance of ModelReferenceType, "
                            "got {0}, class: {1}"
                            .format(field.__class__.__name__, parent_model.__class__.__name__))
                else:
                    l.warning("Unknown field '{0}' in '{1}.prefetch_related'"
                        .format(pr, parent_model.__class__.__name__))
        for pr_field, pr_field_name, ids in prefetch_ids:
            cursor = self.db[pr_field.model_class.MONGO_COLLECTION]\
                .find({"_id": {"$in": ids}})
            pr_data_list = yield cursor.to_list(None)
            # TODO is return models have same order, that provided ids?
            for m, pr_data in zip(results, pr_data_list):
                setattr(m,  pr_field_name, pr_field.model_class(pr_data))
        raise gen.Return(results)
