# -*- coding: utf-8 -*-
from base import BaseTest
from tornado.testing import gen_test
from example_app.models import SchematicsBaseFieldsModel

# TODO: remake for 'id' serialized_name


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
        del self.json_data['_id']

    @gen_test
    def test_serialize_save(self):
        # create model from json
        m = self.model(self.json_data)
        m.validate()
        yield m.save(self.db)
        # check, that model from db corresponds to json data
        find_result = yield m.find(self.db, {"_id": m._id})
        self.assertEqual(len(find_result), 1)
        m_from_db = find_result[0]
        json_from_db = m_from_db.to_primitive()
        del json_from_db['_id']  # ignore _id field
        self.assertEqual(self.json_data, json_from_db)
        # update some field in json
        self.json_data['type_string'] = 'new_value'
        self.json_data["_id"] = str(m._id)
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
