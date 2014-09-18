# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from tornado import gen
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
        main_event_user = yield User.objects.set_db(self.db).get({"id": main_event.user})
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
            main_event, main_event_user=main_event_user,
            prefetched_records='records.event.user',
            prefetched_main_event='event')
        # test two child fields from list field and one child from non-list field
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'main_event.user')\
            .get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, main_event_user=main_event_user,
            prefetched_records='records.event.user',
            prefetched_main_event='event.user')
        # test two child fields from list field, one child from non-list field
        # and one root from another list field
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('records.event.user', 'main_event.user',
                'simplies').get({"id": rs.pk})
        self.assertRecordSeriesEquals(rs_from_db, simplies, records,
            main_event, main_event_user=main_event_user,
            prefetched_records='records.event.user',
            prefetched_main_event='event.user', prefetched_simplies=True)

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
            main_event, main_event_user=None, prefetched_simplies=False,
            prefetched_records='', prefetched_main_event=''):
        for s, s_db in zip(simplies, rs_from_db.simplies):
            if prefetched_simplies:
                self.assertSimpleEqual(s, s_db)
            else:
                self.assertEqual(s.pk, s_db)
        for r, r_db in zip(records, rs_from_db.records):
            if prefetched_records:
                pass
            else:
                self.assertEqual(r.pk, r_db)
        if prefetched_main_event:
            self.assertEventEqual(main_event, rs_from_db.main_event,
                main_event_user, prefetched_main_event)
        else:
            self.assertEqual(main_event.pk, rs_from_db.main_event)

    def assertSimpleEqual(self, simple, simple_from_db):
        self.assertTrue(isinstance(simple_from_db, SimpleModel))
        self.assertEqual(simple.pk, simple_from_db.pk)

    def assertEventEqual(self, event, event_from_db, user, fields):
        self.assertTrue(isinstance(event_from_db, Event))
        self.assertEqual(event_from_db.pk, event.pk)
        self.assertEqual(event_from_db.title, event.title)
        if 'user' in fields:
            self.assertTrue(isinstance(event_from_db.user, User))
            self.assertEqual(event_from_db.user.pk, user.pk)
        else:
            self.assertEqual(event_from_db.user, user.pk)

    def assertRecordEqual(self, record, record_from_db, main_event_user, fields=''):
        self.assertTrue(isinstance(record_from_db, Record))
        self.assertEqual(record_from_db.pk, record.pk)
        if 'event' in fields:
            self.assertEventEqual(record.event, record_from_db.event,
                main_event_user, fields)
        else:
            self.assertEqual(record_from_db.event, record.event.pk)
        # TODO: check json
        # json_from_db = record_from_db.to_primitive()

    @gen.coroutine
    def _create_recordseries(self, records_count=3, simplies_count=2, commit=True):
        simplies = []
        records = []
        for i in range(records_count):
            record, _, _, _ = yield self._create_record()
            records.append(record)
        for i in range(simplies_count):
            sm = yield self._create_simple()
            simplies.append(sm)
        main_event = yield self._create_event()
        rs = RecordSeries(dict(title="rs", main_event=main_event,
            simplies=map(lambda x: x.pk, simplies),
            records=map(lambda x: x.pk, records)))
        if commit:
            yield rs.save(self.db)
        raise gen.Return((rs, simplies, records, main_event))
