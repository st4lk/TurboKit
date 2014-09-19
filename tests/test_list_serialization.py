# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from example_app.models import RecordSeries, SimpleModel, Record, Event, User
from .base import BaseSerializationTest


class TestSerializationReferenceList(BaseSerializationTest):
    MODEL_CLASS = RecordSeries

    @gen_test
    def test_save_get_ids_only(self):
        rs, simplies, records, main_event = yield self._create_recordseries(commit=False)
        yield rs.save(self.db)
        rs_from_db = yield self.model.objects.set_db(self.db).get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_all_ids_only(self):
        data_list = []
        for i in range(3):
            data = yield self._create_recordseries()
            data_list.append(data)
        rss_from_db = yield self.model.objects.set_db(self.db).all()
        self.assertEqual(len(rss_from_db), 3)
        for data, rs_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriesEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_filter_ids_only(self):
        data_list = []
        for i in range(4):
            data = yield self._create_recordseries()
            data_list.append(data)
        ids = [data_list[1][0].pk, data_list[2][0].pk]
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .filter({"id": {"$in": ids}}).all()
        self.assertEqual(len(rss_from_db), 2)
        for data, rs_from_db in zip(
                sorted(data_list[1:3], key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriesEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_prefetch_related_get_list_root_fields(self):
        rs, simplies, records, main_event = yield self._create_recordseries()
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('simplies').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_simplies=True)

    @gen_test
    def test_prefetch_related_all_list_root_fields(self):
        data_list = []
        for i in range(3):
            data = yield self._create_recordseries()
            data_list.append(data)
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('simplies').all()
        for data, rs_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_simplies=True)

    @gen_test
    def test_prefetch_related_filter_list_root_fields(self):
        data_list = []
        for i in range(4):
            data = yield self._create_recordseries()
            data_list.append(data)
        ids = [data_list[1][0].pk, data_list[2][0].pk]
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('simplies').filter({"id": {"$in": ids}}).all()
        self.assertEqual(len(rss_from_db), 2)
        for data, rs_from_db in zip(
                sorted(data_list[1:3], key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriesEquals(rs_from_db, simplies, records,
                main_event, prefetched_simplies=True)

    @gen_test
    def test_prefetch_related_get_list_child_fields(self):
        rs, simplies, records, main_event = yield self._create_recordseries()
        # test one child field from list
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event')
        # test two child fields from list
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event.user')
        # test two child fields from list field and one root from non-list field
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'main_event').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event.user',
            prefetched_main_event='event')
        # test two child fields from list field and one child from non-list field
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'main_event.user')\
            .get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event.user',
            prefetched_main_event='event.user')
        # test two child fields from list field, one child from non-list field
        # and one root from another list field
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'main_event.user',
                'simplies').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event.user',
            prefetched_main_event='event.user', prefetched_simplies=True)
        # test just list of records
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records')

    @gen_test
    def test_prefetch_related_all_list_child_fields(self):
        data_list = []
        for i in range(3):
            data = yield self._create_recordseries()
            data_list.append(data)
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user').all()
        for data, rs_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, prefetched_records='records.event.user')

    @gen_test
    def test_prefetch_related_filter_list_child_fields(self):
        data_list = []
        for i in range(4):
            data = yield self._create_recordseries()
            data_list.append(data)
        ids = [data_list[1][0].pk, data_list[2][0].pk]
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'simplies')\
            .filter({"id": {"$in": ids}}).all()
        self.assertEqual(len(rss_from_db), 2)
        for data, rs_from_db in zip(
                sorted(data_list[1:3], key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriesEquals(rs_from_db, simplies, records,
                main_event, prefetched_records='records.event.user',
                prefetched_simplies=True)

    def assertRecordSeriesEquals(self, rs_from_db, simplies, records,
            main_event, prefetched_simplies=False,
            prefetched_records='', prefetched_main_event=''):
        json_from_db = rs_from_db.to_primitive()
        for s, s_db_data in zip(simplies,
                zip(rs_from_db.simplies, json_from_db['simplies'])):
            s_db, s_db_json = s_db_data
            self.assertSimpleEqual(s, s_db, prefetched=prefetched_simplies,
                json_db=s_db_json)
        for r, r_db_data in zip(records,
                zip(rs_from_db.records, json_from_db['records'])):
            r_db, r_db_json = r_db_data
            self.assertRecordEqual(r, r_db, prefetched_records, json_db=r_db_json)
        if prefetched_main_event:
            self.assertEventEqual(main_event, rs_from_db.main_event,
                prefetched_main_event)
        else:
            self.assertEqual(main_event.pk, rs_from_db.main_event)

    def assertSimpleEqual(self, simple, simple_from_db, prefetched=True,
            json_db=None):
        if prefetched:
            self.assertTrue(isinstance(simple_from_db, SimpleModel))
            self.assertEqual(simple, simple_from_db)
            if json_db:
                self.assertEqual(json_db['id'], str(simple.pk))
        else:
            self.assertEqual(simple.pk, simple_from_db)
            if json_db:
                self.assertEqual(json_db, str(simple.pk))

    def assertEventEqual(self, event, event_from_db, fields, json_db=None):
        self.assertTrue(isinstance(event_from_db, Event))
        self.assertEqual(event_from_db.pk, event.pk)
        self.assertEqual(event_from_db.title, event.title)
        if json_db:
            self.assertEqual(json_db['id'], str(event.pk))
        json_db_user = json_db['user'] if json_db else None
        if 'user' in fields:
            self.assertTrue(isinstance(event_from_db.user, User))
            self.assertEqual(event_from_db.user, event.user)
            if json_db_user:
                self.assertEqual(json_db_user['id'], str(event.user.pk))
        else:
            self.assertEqual(event_from_db.user, event.user.pk)
            if json_db_user:
                self.assertEqual(json_db_user, str(event.user.pk))

    def assertRecordEqual(self, record, record_from_db, fields='', json_db=None):
        if fields:
            self.assertTrue(isinstance(record_from_db, Record))
            self.assertEqual(record_from_db.pk, record.pk)
            if json_db:
                self.assertEqual(json_db['id'], str(record.pk))
            json_db_event = json_db['event'] if json_db else None
            if 'event' in fields:
                self.assertEventEqual(record.event, record_from_db.event,
                    fields, json_db=json_db_event)
            else:
                self.assertEqual(record_from_db.event, record.event.pk)
                if json_db_event:
                    self.assertEqual(json_db_event, str(record.event.pk))
        else:
            self.assertEqual(record.pk, record_from_db)
            if json_db:
                self.assertEqual(json_db, str(record.pk))
