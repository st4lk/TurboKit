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
from schematics.types.compound import ListType
from copy import deepcopy

from .utils import _document_registry
from .transforms import to_mongo, to_primitive, convert
from .types import ObjectIdType, ModelReferenceType, DO_NOTHING
from .managers import AsyncManager
from .signals import pre_save, post_save

l = logging.getLogger(__name__)
MAX_FIND_LIST_LEN = 100


class ExtendedModelOptions(ModelOptions):
    def __init__(self, klass, namespace=None, roles=None, serialize_when_none=True, indexes=None, **other_options):
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
        self.indexes = indexes or tuple()
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
        super(ModelMeta, cls).add_persistence_layer(attrs, name, new_class)
        attrs['_options'].namespace = name.replace("Model", "").lower()
        cls.set_delete_rules(attrs, new_class)
        setattr(new_class, "objects", AsyncManager(new_class, attrs['_options'].namespace))

    @classmethod
    def set_delete_rules(cls, attrs, new_class):
        for field_name, field in attrs['_fields'].iteritems():
            field = field.field if isinstance(field, ListType) else field
            if isinstance(field, ModelReferenceType):
                delete_rule = getattr(field, 'reverse_delete_rule', DO_NOTHING)
                if delete_rule != DO_NOTHING:
                    field.model_class.register_delete_rule(new_class,
                                                     field_name, delete_rule)


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

    def import_data(self, raw_data, **kw):
        """
        Converts and imports the raw data into the instance of the model
        according to the fields in the model.
        Difference with schematics.models.Model.import_data:
        don't delete key with `None` value, so these values will be nullified

        :param raw_data:
            The data to be imported.
        """
        data = self.convert(raw_data, **kw)
        for k in data.keys():
            if data[k] is None and k not in raw_data:
                del data[k]
        self._data.update(data)
        return self

    def partial_import_data(self, raw_data, **kw):
        data = self.convert(raw_data, **kw)
        for k in data.keys():
            if k not in raw_data or (data[k] is None and k not in raw_data):
                del data[k]
        self._data.update(data)
        return self

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

    @gen.coroutine
    def remove(self, db, collection=None):
        """
        Removes current instance from database.
        """
        yield self.objects.set_db(db).remove({"_id": self.pk}, [self])

    @gen.coroutine
    def save(self, db=None, collection=None, ser=None):
        """
        If object has _id, then object will be created or fully rewritten.
        If not, object will be inserted and _id will be assigned.
        Example:
            obj = ExampleModel({"first_name": "Vasya"})
            yield obj.save(self.db)
        """
        yield pre_save.send(self.__class__, document=self)
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
                yield post_save.send(self.__class__, document=self)
                raise gen.Return(self)

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

    @gen.coroutine
    def update(self, db, data, raw=False, **kwargs):
        if not raw:
            data = {"$set": data}
        result = yield self.objects.set_db(db).update({"_id": self.pk},
            data, **kwargs)
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

    @classmethod
    def register_delete_rule(cls, cls_tobe_deleted, field_name, rule):
        delete_rules = getattr(cls._options, 'delete_rules', {})
        delete_rules[(cls_tobe_deleted, field_name)] = rule
        cls._options.delete_rules = delete_rules

    @classmethod
    @gen.coroutine
    def ensure_index(cls, db):  # TODO: check indexes iterability and index dictionarity =)
        for index in deepcopy(cls._options.indexes):
            unique = index.pop('unique', False)
            cache_for = index.pop('cache_for', index.pop('ttl', 300))  # The `ttl` has been deprecated since pymongo 2.3
            fields = index.pop('fields', tuple())

            if not isinstance(fields, basestring):
                fields = map(lambda field: (field, 1) if isinstance(field, basestring) else field, fields)

            l.debug("Creating index for {fields}".format(fields=str(fields)))
            yield db[cls._options.namespace].ensure_index(fields, cache_for, unique=unique, **index)
