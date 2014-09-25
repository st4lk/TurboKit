# -*- coding: utf-8 -*-
import logging
import motor
import pytz

from datetime import timedelta
from tornado import gen, ioloop
from pymongo.errors import ConnectionFailure
from schematics.models import (
    ModelMeta as SchematicsModelMeta,
    Model as SchematicsModel,
    ModelOptions
)

from .utils import methodize, _document_registry
from .transforms import to_mongo, to_primitive, convert
from .types import ObjectIdType
from .managers import AsyncManager

l = logging.getLogger(__name__)
MAX_FIND_LIST_LEN = 100


class ExtendedModelOptions(ModelOptions):
    def __init__(self, klass, namespace=None, roles=None, serialize_when_none=True, **other_options):
        """Extended version of original `ModelOptions`. Designed to store
        more persistence layer options.

        Other options can include keys such as:
            `indexes` to specify indexing strategy,
            `max_documents` and `max_size` for capped collections,
            and `ordering`.

        There will be more in future, maybe. Maybe they will be described
        as standalone params, not just as **kwargs.
        """
        super(ExtendedModelOptions, self).__init__(klass, namespace, roles, serialize_when_none)
        for name, value in other_options.iteritems():
            setattr(self, name, value)

    @property
    def collection(self):
        """Just a handy alias to avoid naming confusion."""
        return self.namespace

    @collection.setter
    def collection(self, value):
        self.namespace = value


class BaseModelMeta(SchematicsModelMeta):
    def __new__(cls, name, bases, attrs):
        super_new = super(BaseModelMeta, cls).__new__
        parents = [b for b in bases if isinstance(b, BaseModelMeta) and b.__mro__ != (b, object)]
        if parents:
            new_class = super_new(cls, name, bases, attrs)
            cls_key = ".".join((new_class.__module__, new_class.__name__))
            _document_registry[cls_key] = new_class  # TODO: move to connection
            new_class._cls_key = cls_key
            cls.add_persistence_layer(attrs, name, new_class)
            return new_class
        return super_new(cls, name, bases, attrs)

    @classmethod
    def add_persistence_layer(cls, attrs, name, new_class):
        pass


class ModelMeta(BaseModelMeta):
    @classmethod
    def _read_options(mcs, name, bases, attrs):
        """Override original `MetaClass` class method to replace `ModelOptions` with
        its extended version as elegant as possible.
        """
        attrs['__optionsclass__'] = attrs.get('__optionsclass__', ExtendedModelOptions)
        return super(BaseModelMeta, mcs)._read_options(name, bases, attrs)

    @classmethod
    def add_persistence_layer(cls, attrs, name, new_class):
        if not attrs['_options'].namespace:
            attrs['_options'].namespace = name.replace("Model", "").lower()

        for attr, value in attrs.iteritems():
            setattr(new_class, attr, value)

        setattr(new_class, "objects", AsyncManager(new_class, attrs['_options'].namespace))


class SerializationMixin(object):

    def __init__(self, raw_data=None, deserialize_mapping=None, strict=True,
            from_mongo=False):
        if raw_data is None:
            raw_data = {}
        self._initial = raw_data
        self._data = self.convert(raw_data, strict=strict,
            mapping=deserialize_mapping, from_mongo=from_mongo)

    def to_mongo(self, role=None, context=None, expand_related=False):
        return to_mongo(self.__class__, self, role=role, context=context)

    def to_primitive(self, role=None, context=None, timezone=None):
        """
        :arg timezone: format instances of LocaleDateTimeType with this timezone
        """
        return to_primitive(self.__class__, self, role=role, context=context,
            timezone=timezone)

    def convert(self, raw_data, **kw):
        """
        Use custom convert function
        """
        return convert(self.__class__, raw_data, **kw)

    @classmethod
    def get_database_timezone(cls):
        return pytz.utc


class SimpleMongoModel(SerializationMixin, SchematicsModel):
    """
    Embedded models must subclass this Model.
    """
    __metaclass__ = BaseModelMeta


class BaseModel(SerializationMixin, SchematicsModel):
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
    __metaclass__ = ModelMeta

    RECONNECT_TRIES = 5  # TODO: Move to connection
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
        return cls._options.namespace

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
