# -*- coding: utf-8 -*-
"""
Base code is taken from https://github.com/wsantos/motorm
"""
import logging
from bson.objectid import ObjectId
from schematics.models import ModelMeta
from tornado import gen
from .cursors import AsyncManagerCursor
from .customtypes import ModelReferenceType

l = logging.getLogger(__name__)


class AsyncManager(object):

    def __init__(self, cls, collection, db=None, prefetch_related=None):
        self.collection = collection
        self.cls = cls
        self.db = db
        self._prefetch_related = set(prefetch_related) if prefetch_related else set()

    @gen.coroutine
    def get(self, query=None, callback=None):
        # TODO: add reconnects

        if 'id' in query:
            _id = query.pop('id')
            query['_id'] = ObjectId(_id) if not isinstance(_id, ObjectId) else _id
        response = yield self.db[self.collection].find_one(query)
        m = self.cls(response)
        for pr in self._prefetch_related:
            field = m._fields.get(pr, None)
            if field:
                if isinstance(field, ModelReferenceType):
                    pr_data = yield self.db[field.model_class.MONGO_COLLECTION]\
                        .find_one({"_id": m[pr]})
                    setattr(m, pr, field.model_class(pr_data))
                else:
                    l.warning("prefetch_related field must be instance of ModelReferenceType, "
                        "got {0}, class: {1}"
                        .format(field.__class__.__name__, m.__class__.__name__))
            else:
                l.warning("Unknown field '{0}' in '{1}.prefetch_related'"
                    .format(pr, m.__class__.__name__))
        raise gen.Return(m)

    def set_db(self, db):
        """
        Create another instance of AsyncManager, so database won't be shared
        between all models. It is thread safe.
        """
        return AsyncManager(self.cls, self.collection, db)

    def prefetch_related(self, *args):
        return AsyncManager(self.cls, self.collection, self.db,
            prefetch_related=self._prefetch_related | set(args))

    def filter(self, query):
        cursor = self.db[self.collection].find(query)
        return AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related)

    @gen.coroutine
    def all(self):
        cursor = self.db[self.collection].find({})
        results = yield AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related).all()
        raise gen.Return(results)


class AsyncManagerMetaClass(ModelMeta):

    def __new__(cls, name, bases, attrs):
        super_new = super(AsyncManagerMetaClass, cls).__new__

        parents = [b for b in bases if isinstance(b, AsyncManagerMetaClass) and
                   not (b.__mro__ == (b, object))]

        if not parents:
            return super_new(cls, name, bases, attrs)
        else:
            new_class = super_new(cls, name, bases, attrs)

            # Collection name
            attrs["MONGO_COLLECTION"] = attrs.get(
                "MONGO_COLLECTION", name.replace("Model", "").lower())

            collection = attrs["MONGO_COLLECTION"]

            # Add all attributes to the class.
            for obj_name, obj in attrs.items():
                setattr(new_class, obj_name, obj)
            manager = AsyncManager(new_class, collection)
            setattr(new_class, "objects", manager)

            return new_class
