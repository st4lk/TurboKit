# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from tornado import gen
from example_app.models import (SchematicsFieldsModel, SimpleModel, User,
    Event, Record, Transaction, Page)
from .base import BaseSerializationTest, BaseTest


class TestSerializationCompound(BaseSerializationTest):
    MODEL_CLASS = SchematicsFieldsModel

    @gen_test
    def test_serialize_save(self):
        # create model from json
        m = self.model(self.json_data)
        yield m.save(self.db)
        # check, that model from db corresponds to json data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        self.assertEqual(self.json_data, json_from_db)
        # update some field in json
        self.json_data["id"] = str(m.pk)
        self.json_data['type_string'] = 'new_value'
        self.json_data['type_list'] = ['str1', 'str2']
        self.json_data['type_dict'] = {'k3': 8, 'k4': 9}
        self.json_data['type_list_of_dict'] = [{'k1': 'str1'}, {'k2': 'str2'}]
        self.json_data['type_dict_of_list'] = {'k6': [1, 2], 'k7': [88, ]}
        self.json_data['type_model']['type_string'] = 'new_model_string'
        self.json_data['type_list_model'] = [
            {'type_string': 'ss1', 'type_int': 1},
            {'type_string': 'ss2', 'type_int': 2},
        ]
        # create model from that json and save it to db
        m_updated = self.model(self.json_data)
        yield m_updated.save(self.db)
        # check, that existed object in db corresponds to new json data
        self.assertEqual(m.pk, m_updated.pk)
        find_result = yield self.model.objects.set_db(self.db)\
            .filter({"_id": m_updated.pk}).all()
        self.assertEqual(len(find_result), 1)
        m_updated_from_db = find_result[0]
        json_updated_from_db = m_updated_from_db.to_primitive()
        self.assertEqual(self.json_data, json_updated_from_db)

    @gen_test
    def test_serialize_update(self):
        # create model from json
        m = self.model(self.json_data)
        yield m.save(self.db)
        # update only some fields from model instance
        updated_json = {
            'type_string': 'updated_string',
            'type_email': 'updated@email.com',
            'type_long': 12344555,
            'type_datetime': '2014-09-11T12:44:30.210000',
            'type_list': ['str1', 'str2'],
            'type_dict': {'k3': 8, 'k4': 9},
            'type_list_of_dict': [{'k1': 'str1'}, {'k2': 'str2'}],
            'type_dict_of_list': {'k6': [1, 2], 'k7': [88, ]},
            'type_model': {'type_string': "new_model_string"},
            'type_list_model': [
                {'type_string': 'ss1', 'type_int': 1},
                {'type_string': 'ss2', 'type_int': 2},
            ]
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
            "type_url": "http://ya.ru",
            'type_list_of_dict': [{'k8': 'str8'}, {'k9': 'str9'}],
            'type_dict_of_list': {'k89': [8, 9], 'k567': [0, 8]},
            'type_model': {'type_int': 88},
            'type_list_model': [
                {'type_string': 'ss1', 'type_int': 1},
            ]
        }
        yield m.__class__.update(self.db, {"_id": m.pk}, updated_json_for_cls)
        # check, that model from db corresponds to updated_json_for_cls data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        expected_cls_json = dict(expected_json)
        expected_cls_json.update(updated_json_for_cls)
        self.assertEqual(json_from_db, expected_cls_json)


class TestSerializationModelReference(BaseSerializationTest):
    MODEL_CLASS = SchematicsFieldsModel

    @gen_test
    def test_ref_serialize_save(self):
        m, sm, um = yield self._create_model_with_ref_model()
        # check, that model from db corresponds to json data
        json_from_db = yield self._get_json_from_db_and_check_count(m)
        self.json_data['type_ref_simplemodel'] = str(sm.pk)
        self.json_data['type_ref_usermodel'] = str(um.pk)
        self.assertEqual(self.json_data, json_from_db)
        m_from_db = yield self.model.objects.set_db(self.db).get({"id": m.pk})
        self.assertEqual(m_from_db.type_ref_simplemodel, sm.pk)

    @gen_test
    def test_prefetch_related_get_root_fields(self):
        m, sm, um = yield self._create_model_with_ref_model()
        model_qs = self.model.objects.set_db(self.db)
        # check one reference field
        m_from_db = yield model_qs.prefetch_related('type_ref_simplemodel')\
            .get({"id": m.pk})
        self.assertRelatedModelFetched(m, m_from_db, sm, 'type_ref_simplemodel',
            SimpleModel)
        self.assertEqual(m_from_db.type_ref_usermodel, um.pk)
        # check two reference field
        m_from_db = yield model_qs.prefetch_related('type_ref_simplemodel',
            'type_ref_usermodel').get({"id": m.pk})
        self.assertRelatedModelFetched(m, m_from_db, sm, 'type_ref_simplemodel',
            SimpleModel)
        self.assertRelatedModelFetched(m, m_from_db, um, 'type_ref_usermodel',
            User)

    @gen_test
    def test_prefetch_related_all_root_fields(self):
        created_models = yield self._create_models()
        model_qs = self.model.objects.set_db(self.db)
        # check one reference field
        mdls_from_db = yield model_qs.prefetch_related('type_ref_simplemodel').all()
        for m_created, m_from_db in zip(
                sorted(created_models, key=lambda x: x[0].pk),
                sorted(mdls_from_db, key=lambda x: x.pk)):
            m = m_created[0]
            sm = m_created[1]
            um = m_created[2]
            self.assertRelatedModelFetched(m, m_from_db, sm, 'type_ref_simplemodel',
                SimpleModel)
            self.assertEqual(m.pk, m_from_db.pk)
            self.assertEqual(m_from_db.type_ref_usermodel, um.pk)
            self.assertEqual(m_from_db.to_mongo()['type_ref_simplemodel'], sm.pk)
        # check two reference field
        mdls_from_db = yield model_qs.prefetch_related(
            'type_ref_simplemodel', 'type_ref_usermodel').all()
        for m_created, m_from_db in zip(
                sorted(created_models, key=lambda x: x[0].pk),
                sorted(mdls_from_db, key=lambda x: x.pk)):
            m = m_created[0]
            sm = m_created[1]
            um = m_created[2]
            self.assertRelatedModelFetched(m, m_from_db, sm, 'type_ref_simplemodel',
                SimpleModel)
            self.assertRelatedModelFetched(m, m_from_db, um, 'type_ref_usermodel',
                User)
            self.assertEqual(m.pk, m_from_db.pk)
            self.assertEqual(m_from_db.to_mongo()['type_ref_simplemodel'], sm.pk)
            self.assertEqual(m_from_db.to_mongo()['type_ref_usermodel'], um.pk)

    @gen_test
    def test_prefetch_related_filter_root_fields(self):
        created_models = yield self._create_models()
        for i, cm in enumerate(created_models, start=1):
            cm[0].type_int = i
            yield cm[0].save(self.db)
        model_qs = self.model.objects.set_db(self.db).filter({"type_int": {"$lte": 2}})
        # check two reference field
        mdls_from_db = yield model_qs.prefetch_related(
            'type_ref_simplemodel', 'type_ref_usermodel').all()
        self.assertEqual(len(mdls_from_db), 2)
        for m_created, m_from_db in zip(
                sorted(created_models[:2], key=lambda x: x[0].pk),
                sorted(mdls_from_db, key=lambda x: x.pk)):
            m = m_created[0]
            sm = m_created[1]
            um = m_created[2]
            self.assertRelatedModelFetched(m, m_from_db, sm, 'type_ref_simplemodel',
                SimpleModel)
            self.assertRelatedModelFetched(m, m_from_db, um, 'type_ref_usermodel',
                User)
            self.assertEqual(m.pk, m_from_db.pk)
            self.assertEqual(m_from_db.to_mongo()['type_ref_simplemodel'], sm.pk)
            self.assertEqual(m_from_db.to_mongo()['type_ref_usermodel'], um.pk)

    @gen_test
    def test_prefetch_related_get_child_fields(self):
        record, sm, event, user = yield self._create_record()
        record_from_db = yield Record.objects.set_db(self.db)\
            .prefetch_related('event.user', 'simple').get({"id": record.pk})
        self.assertChildRelatedModelFetched(record, record_from_db, event, user, sm)

    @gen_test
    def test_prefetch_related_all_child_fields(self):
        data_list = []
        for i in range(3):
            record, sm, event, user = yield self._create_record()
            data_list.append((record, sm, event, user))
        records_from_db = yield Record.objects.set_db(self.db)\
            .prefetch_related('event.user', 'simple').all()
        for data, record_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(records_from_db, key=lambda x: x.pk)):
            record, sm, event, user = data
            self.assertEqual(record.pk, record_from_db.pk)
            self.assertChildRelatedModelFetched(record, record_from_db, event, user, sm)

    @gen_test
    def test_prefetch_related_filter_child_fields(self):
        data_list = []
        for i in range(5):
            record, sm, event, user = yield self._create_record(event_title=str(i))
            data_list.append((record, sm, event, user))
        records_from_db = yield Record.objects.set_db(self.db)\
            .filter({"title": {"$lte": "2"}})\
            .prefetch_related('event.user', 'simple').all()
        self.assertEqual(len(records_from_db), 3)
        for data, record_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(records_from_db, key=lambda x: x.pk)):
            record, sm, event, user = data
            self.assertEqual(record.pk, record_from_db.pk)
            self.assertChildRelatedModelFetched(record, record_from_db, event, user, sm)

    def assertRelatedModelFetched(self, m_source, m_from_db, ref_model,
            ref_model_field_name, ref_m_class):
        ref_model_field = getattr(m_from_db, ref_model_field_name)
        self.assertTrue(isinstance(ref_model_field, ref_m_class))
        self.assertEqual(ref_model_field.pk, ref_model.pk)
        json_from_db = m_from_db.to_primitive()
        self.assertEqual(json_from_db['id'], str(m_source.pk))
        self.assertEqual(json_from_db[ref_model_field_name], ref_model.to_primitive())

    def assertChildRelatedModelFetched(self, record, record_from_db, event,
            user, sm):
        self.assertTrue(isinstance(record_from_db.event, Event))
        self.assertEqual(record_from_db.event.pk, event.pk)
        self.assertTrue(isinstance(record_from_db.event.user, User))
        self.assertEqual(record_from_db.event.user.pk, user.pk)
        self.assertTrue(isinstance(record_from_db.simple, SimpleModel))
        self.assertEqual(record_from_db.simple.pk, sm.pk)
        json_from_db = record_from_db.to_primitive()
        self.assertEqual(json_from_db['id'], str(record.pk))
        self.assertEqual(json_from_db['event']['id'], str(event.pk))
        self.assertEqual(json_from_db['simple']['id'], str(sm.pk))
        self.assertEqual(json_from_db['event']['user']['id'], str(user.pk))

    @gen.coroutine
    def _create_model_with_ref_model(self):
        sm = yield self._create_simple()
        um = yield self._create_user()
        m = self.model(self.json_data)
        m.type_ref_simplemodel = sm
        m.type_ref_usermodel = um
        yield m.save(self.db)
        raise gen.Return((m, sm, um))

    @gen.coroutine
    def _create_models(self, amount=5):
        results = []
        for i in range(5):
            m, sm, um = yield self._create_model_with_ref_model()
            results.append((m, sm, um))
        raise gen.Return(results)


class TestSerializationSingleDynamicType(BaseSerializationTest):
    """
    Currently can't create dynamic NestedModel from json as value for dynamic field
    """
    MODEL_CLASS = Page

    @gen_test
    def test_single_dynamic_save(self):
        json_data = self.json_data
        content_list = [
            12,
            'some string',
            ['l', 'i', 's', 't', 1],
            ['l', 'i', 's', 't', [1, 2]],
            {'d': 'i', 'c': 't'},
        ]
        for content in content_list:
            json_data['content'] = content
            # create model from json
            m = self.model(json_data)
            yield m.save(self.db)
            m_db = yield self.model.objects.set_db(self.db).get({"id": m.pk})
            self.assertEqual(m_db.content, content)
            db_json = m_db.to_primitive()
            # ignore id
            del db_json['id']
            self.assertEqual(db_json, json_data)


class TestSerializationGenericModelReference(BaseTest):
    # TODO
    MODEL_CLASS = Transaction

    @gen_test
    def test_generic_model_serialize_save(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_get_root_fields(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_all_root_fields(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_filter_root_fields(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_get_child_fields(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_all_child_fields(self):
        pass

    @gen_test
    def test_generic_model_prefetch_related_filter_child_fields(self):
        pass
