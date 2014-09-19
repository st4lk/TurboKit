# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from example_app.models import RecordSeries
from .base import BaseSerializationTest


class TestAggregate(BaseSerializationTest):
    MODEL_CLASS = RecordSeries

    @gen_test
    def test_aggregation_unwind(self):
        simplies_mapping = {}
        for i in range(3):
            rs, simplies, _, _ = yield self._create_recordseries()
            for s in simplies:
                simplies_mapping[s.pk] = rs.pk
        unwind_db = yield self.model.objects.set_db(self.db)\
            .aggregate([{"$project": {"simplies": 1}}, {"$unwind": "$simplies"}])
        self.assertEqual(len(unwind_db), len(simplies_mapping))
        for unwind_data in unwind_db:
            s_id = unwind_data['simplies']
            rs_id = unwind_data['_id']
            self.assertEqual(simplies_mapping[s_id], rs_id)
