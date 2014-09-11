# -*- coding: utf-8 -*-
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app.models import SchematicsBaseFieldsModel


class TestSerializationOperations(BaseTest):

    def setUp(self):
        super(TestSerializationOperations, self).setUp()
        self.db = self.default_db
        self.model = SchematicsBaseFieldsModel
        m_temp = self.model()
        for fname, fobj in filter(lambda n: n[0] != "_id", m_temp._fields.iteritems()):
            value = fobj._mock()
            if 'type_geopoint' in fname:
                value = list(value)  # convert tuple to list, as list != tuple
            setattr(m_temp, fname, value)
        self.json_data = m_temp.to_primitive()
        del self.json_data['id']

    @gen_test
    def test_serialize_save(self):
        # create model from json
        m = self.model(self.json_data)
        m.validate()
        yield m.save(self.db)
        # check, that model from db corresponds to json data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        self.assertEqual(self.json_data, json_from_db)
        # update some field in json
        self.json_data['type_string'] = 'new_value'
        self.json_data["id"] = str(m._id)
        # create model from that json and save it to db
        m_updated = self.model(self.json_data)
        yield m_updated.save(self.db)
        # check, that existed object in db corresponds to new json data
        self.assertEqual(m._id, m_updated._id)
        find_result = yield m.find(self.db, {"_id": m_updated._id})
        self.assertEqual(len(find_result), 1)
        m_updated_from_db = find_result[0]
        json_updated_from_db = m_updated_from_db.to_primitive()
        self.assertEqual(self.json_data, json_updated_from_db)

    @gen_test
    def test_serialize_update(self):
        # create model from json
        m = self.model(self.json_data)
        m.validate()
        yield m.save(self.db)
        # update only some fields from model instance
        updated_json = {
            'type_string': 'updated_string',
            'type_email': 'updated@email.com',
            'type_long': 12344555,
            'type_datetime': '2014-09-11T12:44:30.210000',
        }
        yield m.update(self.db, updated_json)
        # check, that model from db corresponds to updated_json data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        expected_json = dict(self.json_data)
        expected_json.update(updated_json)
        self.assertEqual(json_from_db, expected_json)
        # update only some fields from model class
        updated_json_for_cls = {
            'type_string': 'updated_string_class',
            "type_url": "http://ya.ru"
        }
        yield m.__class__.update(self.db, {"_id": m._id}, updated_json_for_cls)
        # check, that model from db corresponds to updated_json_for_cls data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        expected_cls_json = dict(expected_json)
        expected_cls_json.update(updated_json_for_cls)
        self.assertEqual(json_from_db, expected_cls_json)

    @gen.coroutine
    def _get_json_from_db_and_check_count(self, obj, count=1):
        find_result = yield obj.find(self.db, {"_id": obj._id})
        self.assertEqual(len(find_result), count)
        m_from_db = find_result[0]
        json_from_db = m_from_db.to_primitive()
        del json_from_db['id']  # ignore id field
        raise gen.Return(json_from_db)
