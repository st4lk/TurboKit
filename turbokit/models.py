# -*- coding: utf-8 -*-
import logging
import motor
from datetime import timedelta
from tornado import gen, ioloop
from schematics.models import ModelMeta, Model as SchematicsModel
from pymongo.errors import ConnectionFailure
from .utils import methodize, _document_registry
from .transforms import to_mongo
from .types import ObjectIdType
from .managers import AsyncManagerMetaClass

l = logging.getLogger(__name__)
MAX_FIND_LIST_LEN = 100


class SimpleModelMetaClass(ModelMeta):

    def __new__(cls, name, bases, attrs):
        super_new = super(SimpleModelMetaClass, cls).__new__

        parents = [b for b in bases if isinstance(b, SimpleModelMetaClass) and
                   not (b.__mro__ == (b, object))]

        if not parents:
            return super_new(cls, name, bases, attrs)
        else:
            new_class = super_new(cls, name, bases, attrs)
            cls_key = ".".join((new_class.__module__, new_class.__name__))
            _document_registry[cls_key] = new_class
            new_class._cls_key = cls_key

            return new_class


class MongoDBMixin(object):
    def to_mongo(self, role=None, context=None, expand_related=False):
        return to_mongo(self.__class__, self, role=role, context=context)


class SimpleMongoModel(MongoDBMixin, SchematicsModel):
    """
    Embedded models must subclass this Model.
    """
    __metaclass__ = SimpleModelMetaClass


class BaseModel(MongoDBMixin, SchematicsModel):
    """
    Provides generic methods to work with model.
    Why use `MyModel.find_one` instead of `db.collection.find_one` ?
    1. Collection name is declared inside the model, so it is not needed
        to provide it all the time
    2. This `MyModel.find_one` will return MyModel instance, whereas
        `db.collection.find_one` will return dictionary

    Example with directly access:

        db_result = yield motor.Op(db.collection_name.find_one({"i": 3}))
        obj = MyModel(db_result)

    Same example, but using MyModel.find_one:

        obj = yield MyModel.find_one(db, {"i": 3})
    """
    __metaclass__ = AsyncManagerMetaClass

    RECONNECT_TRIES = 5
    RECONNECT_TIMEOUT = 2  # in seconds

    _id = ObjectIdType(serialized_name='id')

    def __init__(self, *args, **kwargs):
        self.set_db(kwargs.pop('db', None))
        self.update = methodize(self.__class__._update_instance, self)
        # TODO allow to set model instance for ModelReferenceType, not only id
        super(BaseModel, self).__init__(*args, **kwargs)

    @property
    def db(self):
        return getattr(self, '_db', None)

    def set_db(self, db):
        self._db = db

    @classmethod
    def process_query(cls, query):
        """
        query can be modified here before actual providing to database.
        """
        return dict(query)

    @property
    def pk(self):
        return self._id

    @classmethod
    def get_collection(cls):
        return cls.MONGO_COLLECTION

    @classmethod
    def check_collection(cls, collection):
        return collection or cls.get_collection()

    @classmethod
    @gen.coroutine
    def remove_entries(cls, db, query, collection=None):
        """
        TODO: move to model manager
        Removes documents by given query.
        Example:
            obj = yield ExampleModel.remove_entries(
                self.db, {"first_name": "Hello"})
        """
        c = cls.check_collection(collection)
        query = cls.process_query(query)
        for i in cls.reconnect_amount():
            try:
                yield motor.Op(db[c].remove, query)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(i,
                    'remove_entries')
                if exceed:
                    raise e
            else:
                return

    @gen.coroutine
    def remove(self, db, collection=None):
        """
        Removes current instance from database.
        Example:
            obj = yield ExampleModel.find_one(self.db, {"last_name": "Sara"})
            yield obj.remove(self.db)
        """
        yield self.remove_entries(db, {"_id": self.pk}, collection)

    @gen.coroutine
    def save(self, db=None, collection=None, ser=None):
        """
        If object has _id, then object will be created or fully rewritten.
        If not, object will be inserted and _id will be assigned.
        Example:
            obj = ExampleModel({"first_name": "Vasya"})
            yield obj.save(self.db)
        """
        self.validate()
        db = db or self.db
        c = self.check_collection(collection)
        data = self.get_data_for_save(ser)
        result = None
        for i in self.reconnect_amount():
            try:
                result = yield motor.Op(db[c].save, data)
            except ConnectionFailure as e:
                exceed = yield self.check_reconnect_tries_and_wait(i, 'save')
                if exceed:
                    raise e
            else:
                if result:
                    self._id = result
                return

    @gen.coroutine
    def insert(self, db=None, collection=None, ser=None, **kwargs):
        """
        If object has _id, then object will be inserted with given _id.
        If object with such _id is already in database, then
        pymongo.errors.DuplicateKeyError will be raised.
        If object has no _id, then object will be inserted and _id will be
        assigned.

        Example:
            obj = ExampleModel({"first_name": "Vasya"})
            yield obj.insert()
        """
        db = db or self.db
        c = self.check_collection(collection)
        data = self.get_data_for_save(ser)
        for i in self.reconnect_amount():
            try:
                result = yield motor.Op(db[c].insert, data, **kwargs)
            except ConnectionFailure as e:
                exceed = yield self.check_reconnect_tries_and_wait(i, 'insert')
                if exceed:
                    raise e
            else:
                if result:
                    self._id = result
                return

    @classmethod
    @gen.coroutine
    def update(cls, db, query, ser, collection=None, update=None,
            upsert=False, multi=False):
        """
        Update only given fields for object, found by query.
        Other args are the same, as in pymongo.update:
        http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update
        """
        c = cls.check_collection(collection)
        data = cls.get_data_for_update(ser)
        data = {"$set": data}
        for i in cls.reconnect_amount():
            try:
                result = yield motor.Op(db[c].update,
                    query, data, upsert=upsert, multi=multi)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(i,
                    'update')
                if exceed:
                    raise e
            else:
                l.debug("Update result: {0}".format(result))
                raise gen.Return(result)

    @gen.coroutine
    def _update_instance(self, db, ser, **kwargs):
        """
        This method will be invoked, when `.update` is called from model instance.
        This is a helper for cls.update, but it will pass query {"_id": self.id}
        automatically.
        """
        result = yield self.__class__.update(db, {"_id": self.pk}, ser, **kwargs)
        raise gen.Return(result)

    @classmethod
    def reconnect_amount(cls):
        return xrange(cls.RECONNECT_TRIES + 1)

    @classmethod
    @gen.coroutine
    def check_reconnect_tries_and_wait(cls, reconnect_number, func_name):
        if reconnect_number >= cls.RECONNECT_TRIES:
            raise gen.Return(True)
        else:
            timeout = cls.RECONNECT_TIMEOUT
            l.warning("ConnectionFailure #{0} in {1}.{2}. Waiting {3} seconds"
                .format(
                    reconnect_number + 1, cls.__name__, func_name, timeout))
            io_loop = ioloop.IOLoop.instance()
            yield gen.Task(io_loop.add_timeout, timedelta(seconds=timeout))

    def get_data_for_save(self, ser=None):
        """
        Prepare data to be send to mongo
        :arg ser: if this field is not empty, data will be taken from it
        """
        data = ser or self.to_mongo()
        return data

    @classmethod
    def _flatten_data(cls, data, nested=None, new_data=None):
        """
        Transforms data with nested dict:
        {
            'k1': 'v1',
            'k2': {
                'k3': 'v3',
                'k4': 'v4',
            }
            'k5': [
                {'k6': 'v6'},
                {'k7': 'v7'},
            ]
        }
        Into flatten data (TODO: flatten dict in list also, can be issues):
        {
            'k1': 'v1',
            'k2.k3': 'v3',
            'k2.k4': 'v4',
            'k5': [
                {'k6': 'v6'},
                {'k7': 'v7'},
            ]
        }
        """
        if new_data is None:
            new_data = {}
        if nested:
            nested_data = nested[0]
            root_key = nested[1]
            for k, v in nested_data.iteritems():
                nested_key = ".".join((root_key, k))
                if isinstance(v, dict):
                    new_data.update(cls._flatten_data(data, nested=[v, nested_key]))
                else:
                    new_data[nested_key] = v
        else:
            for k, v in data.iteritems():
                if isinstance(v, dict):
                    new_data.update(
                        cls._flatten_data(data, nested=[v, k], new_data=new_data))
                else:
                    new_data[k] = v
        return new_data

    @classmethod
    def get_data_for_update(cls, data, flatten_data=False):
        if flatten_data:
            data = cls._flatten_data(data)
        return data
