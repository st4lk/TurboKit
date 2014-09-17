# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from tornado import gen
from example_app.models import (SimpleModel, User, Event, Record, RecordSeries)
from .base import BaseSerializationTest


class TestSerializationReferenceList(BaseSerializationTest):
    MODEL_CLASS = RecordSeries

    @gen_test
    def test_prefetch_related_get_list_root_fields(self):
        # TODO
        pass
