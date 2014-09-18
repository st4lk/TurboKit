# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from tornado import gen
from example_app.models import RecordSeries, SimpleModel
from .base import BaseSerializationTest


class TestSerializationReferenceList(BaseSerializationTest):
    MODEL_CLASS = RecordSeries

    @gen_test
    def test_save_get_ids_only(self):
        rs, simplies, records, main_event = yield self._create_recordseires(commit=False)
        yield rs.save(self.db)
        rs_from_db = yield self.model.objects.set_db(self.db).get({"id": rs.pk})
        self.assertRecordSeriasEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_all_ids_only(self):
        data_list = []
        for i in range(3):
            data = yield self._create_recordseires()
            data_list.append(data)
        rss_from_db = yield self.model.objects.set_db(self.db).all()
        self.assertEqual(len(rss_from_db), 3)
        for data, rs_from_db in zip(
                sorted(data_list, key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriasEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_filter_ids_only(self):
        data_list = []
        for i in range(4):
            data = yield self._create_recordseires()
            data_list.append(data)
        ids = [data_list[1][0].pk, data_list[2][0].pk]
        rss_from_db = yield self.model.objects.set_db(self.db)\
            .filter({"id": {"$in": ids}}).all()
        self.assertEqual(len(rss_from_db), 2)
        for data, rs_from_db in zip(
                sorted(data_list[1:3], key=lambda x: x[0].pk),
                sorted(rss_from_db, key=lambda x: x.pk)):
            rs, simplies, records, main_event = data
            self.assertRecordSeriasEquals(rs_from_db, simplies, records, main_event)

    @gen_test
    def test_prefetch_related_get_list_root_fields(self):
        rs, simplies, records, main_event = yield self._create_recordseires()
        rs_from_db = yield self.model.objects.set_db(self.db)\
            .prefetch_related('simplies').get({"id": rs.pk})
        self.assertRecordSeriasEquals(rs_from_db, simplies, records,
            main_event, prefetched_simplies=True)

    def assertRecordSeriasEquals(self, rs_from_db, simplies, records,
            main_event, prefetched_records=False, prefetched_simplies=False):
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
        self.assertEqual(main_event.pk, rs_from_db.main_event)

    def assertSimpleEqual(self, simple, simple_from_db):
        self.assertTrue(isinstance(simple_from_db, SimpleModel))
        self.assertEqual(simple.pk, simple_from_db.pk)

    @gen.coroutine
    def _create_recordseires(self, records_count=3, simplies_count=2, commit=True):
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
