# -*- coding: utf-8 -*-
"""
Base code is taken from https://github.com/wsantos/motorm
"""
import logging
from bson.objectid import ObjectId
from schematics.models import ModelMeta
from tornado import gen
from schematics.models import Model as SchematicsModel
from pymongo.errors import OperationFailure
from .cursors import AsyncManagerCursor, PrefetchRelatedMixin
from .utils import _document_registry

l = logging.getLogger(__name__)


class AsyncManager(PrefetchRelatedMixin):

    def __init__(self, cls, collection, db=None, **kwargs):
        super(AsyncManager, self).__init__(cls, collection, db=db, **kwargs)
        self.collection = collection
        self.cls = cls
        self.db = db

    def set_db(self, db):
        """
        Create another instance of AsyncManager, so database won't be shared
        between all models. It is thread safe.
        """
        return AsyncManager(self.cls, self.collection, db)

    @gen.coroutine
    def get(self, query=None, callback=None):
        # TODO: add reconnects here and in other methods
        query = self.process_query(query)
        response = yield self.db[self.collection].find_one(query)
        m = self.cls(response)

        results_with_related = yield self.fetch_related_objects([m])
        raise gen.Return(results_with_related[0])

    @gen.coroutine
    def all(self):
        cursor = self.db[self.collection].find({})
        results = yield AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related).all()
        raise gen.Return(results)

    @gen.coroutine
    def aggregate(self, pipeline, **kwargs):
        """
        With server version >= 2.5.1, pass cursor={} to retrieve unlimited
        aggregation results with a CommandCursor
        """
        result = yield self.db[self.collection].aggregate(pipeline, **kwargs)
        if 'cursor' not in kwargs:
            if result['ok'] != 1:
                raise OperationFailure(result, code=result['ok'])
            result = result['result']
        raise gen.Return(result)

    def prefetch_related(self, *args):
        return AsyncManager(self.cls, self.collection, self.db,
            prefetch_related=self._prefetch_related | set(args))

    def filter(self, query):
        query = self.process_query(query)
        cursor = self.db[self.collection].find(query)
        return AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related)

    def process_query(self, query):
        if 'id' in query:
            _id = query.pop('id')
            if isinstance(_id, SchematicsModel):
                _id = _id.pk
            if not isinstance(_id, ObjectId) and not isinstance(_id, dict):
                query['_id'] = ObjectId(_id)
            else:
                query['_id'] = _id
        return query


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

            cls_key = ".".join((new_class.__module__, new_class.__name__))
            _document_registry[cls_key] = new_class
            new_class._cls_key = cls_key

            return new_class
