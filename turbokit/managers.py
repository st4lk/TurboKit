# -*- coding: utf-8 -*-
"""
Base code is taken from https://github.com/wsantos/motorm
"""
# from tornado.gen import coroutine
from bson.objectid import ObjectId
from schematics.models import ModelMeta
from tornado.concurrent import return_future
from .cursors import AsyncManagerCursor


class AsyncManager(object):

    def __init__(self, cls, collection, db=None):
        self.collection = collection
        self.cls = cls
        self.db = db

    @return_future
    def get(self, query=None, callback=None):
        # TODO: add reconnects

        def handle_get_response(response, error):
            if error:
                raise error
            else:
                if response is None:
                    callback(None)
                else:
                    callback(self.cls(response))
        if query is None:
            query = {}

        if 'id' in query:
            _id = query.pop('id')
            query['_id'] = ObjectId(_id) if not isinstance(_id, ObjectId) else _id
        self.db[self.collection].find_one(query, callback=handle_get_response)

    def set_db(self, db):
        """
        Create another instance of AsyncManager, so database won't be shared
        between all models. It is thread safe.
        """
        return AsyncManager(self.cls, self.collection, db)

    def filter(self, query):
        cursor = self.db[self.collection].find(query)
        return AsyncManagerCursor(self.cls, cursor)

    @return_future
    def all(self, callback):
        cursor = self.db[self.collection].find({})
        AsyncManagerCursor(self.cls, cursor).all(callback)


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
