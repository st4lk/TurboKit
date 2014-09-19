# -*- coding: utf-8 -*-
import string
import random
import motor
import collections
from random import randint
from tornado.testing import AsyncHTTPTestCase
from tornado.ioloop import IOLoop
from tornado import gen
from example_app.app import AppODM
from schematics import types
from schematics.types import compound
from example_app.models import SimpleModel, User, Event, Record


class BaseTest(AsyncHTTPTestCase):
    DATABASES = ['odm_test', ]

    def setUp(self):
        super(BaseTest, self).setUp()
        self.db_clear()

    def tearDown(self):
        super(BaseTest, self).tearDown()
        self.db_clear()

    def get_app(self):
        return AppODM()

    def get_new_ioloop(self):
        return IOLoop.instance()

    @property
    def mongo_client(self):
        return self._app.settings['mongo_client']

    @property
    def db(self):
        return self.mongo_client[self.DATABASES[0]]

    def db_clear(self):
        @gen.engine
        def async_op(dname):
            yield motor.Op(self.mongo_client.drop_database, dname)
            self.stop()
        for dname in self.DATABASES:
            async_op(dname)
            self.wait()

    @staticmethod
    def get_random_string(size=10, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))


class BaseSerializationTest(BaseTest):
    MODEL_CLASS = None

    def setUp(self):
        super(BaseSerializationTest, self).setUp()
        self.model = self.MODEL_CLASS
        m_temp = self.model()
        self.json_data = self._get_mocked_data(m_temp)

    def _mock_field_value(self, field_object):
        if isinstance(field_object, compound.ListType):
            value = []
            for i in range(3):
                value.append(self._mock_field_value(field_object.field))
        elif isinstance(field_object, compound.DictType):
            value = {}
            for k in ['key1', 'key2']:
                value[k] = self._mock_field_value(field_object.field)
        elif isinstance(field_object, compound.ModelType):
            value = self._get_mocked_data(field_object.model_class())
        else:
            value = field_object._mock()
            if isinstance(field_object, types.GeoPointType):
                value = list(value)  # convert tuple to list, as list != tuple
        return value

    def _get_mocked_data(self, obj):
        for fname, fobj in filter(lambda n: n[0] != "_id", obj._fields.iteritems()):
            value = self._mock_field_value(fobj)
            setattr(obj, fname, value)
        json_data = obj.to_primitive()
        json_data.pop('id', None)
        return json_data

    def _update_dict_recursive(self, d, u):
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                r = self._update_dict_recursive(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        return d

    @gen.coroutine
    def _get_json_from_db_and_check_count(self, instance, count=1):
        find_result = yield instance.objects.set_db(self.db)\
            .filter({"_id": instance.pk}).all()
        self.assertEqual(len(find_result), count)
        m_from_db = find_result[0]
        json_from_db = m_from_db.to_primitive()
        del json_from_db['id']  # ignore id field
        raise gen.Return(json_from_db)

    @gen.coroutine
    def _create_user(self):
        um = User(dict(name=self.get_random_string(), age=randint(20, 50)))
        um.validate()
        yield um.save(self.db)
        raise gen.Return(um)

    @gen.coroutine
    def _create_simple(self):
        sm = SimpleModel(dict(title=self.get_random_string(),
            secret=self.get_random_string()))
        sm.validate()
        yield sm.save(self.db)
        raise gen.Return(sm)

    @gen.coroutine
    def _create_event(self, user=None):
        if user is None:
            user = yield self._create_user()
        event = Event(dict(title=self.get_random_string(), user=user))
        event.validate()
        yield event.save(self.db)
        raise gen.Return(event)

    @gen.coroutine
    def _create_record(self, event_title=None):
        event_title = event_title or self.get_random_string()
        user = yield self._create_user()
        event = yield self._create_event(user)
        sm = yield self._create_simple()
        record = Record(dict(title=event_title, event=event, simple=sm))
        yield record.save(self.db)
        raise gen.Return((record, sm, event, user))
