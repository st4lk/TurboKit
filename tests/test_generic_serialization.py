# -*- coding: utf-8 -*-
from tornado.testing import gen_test
from example_app.models import RecordSeries, SimpleModel, Record, Event, User
from .base import BaseSerializationTest


class TestSerializationGenericModelType(BaseSerializationTest):
    MODEL_CLASS = RecordSeries
