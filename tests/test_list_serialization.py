# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from tornado import gen
from example_app.models import RecordSeries
from .base import BaseSerializationTest


class TestSerializationReferenceList(BaseSerializationTest):
    MODEL_CLASS = RecordSeries

    @gen_test
    def test_save_get_ids_only(self):
        rs, simplies, records, main_event = yield self._create_recordseires(commit=False)
        yield rs.save(self.db)
        rs_from_db = yield self.model.objects.set_db(self.db).get({"id": rs.pk})
        for s, s_db_id in zip(simplies, rs_from_db.simplies):
            self.assertEqual(s.pk, s_db_id)
        for r, r_db_id in zip(records, rs_from_db.records):
            self.assertEqual(r.pk, r_db_id)
        self.assertEqual(main_event.pk, rs_from_db.main_event)

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
