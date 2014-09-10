# -*- coding: utf-8 -*-
import motor
from tornado.testing import AsyncHTTPTestCase
from tornado.ioloop import IOLoop
from tornado import gen
from example_app.app import AppODM


class BaseTest(AsyncHTTPTestCase):
    DATABASES = ['odm_test', ]

    def setUp(self):
        super(BaseTest, self).setUp()
        self.db_clear()

    def tearDown(self):
        super(BaseTest, self).tearDown()
        self.db_clear()

    def get_app(self):
        return AppODM()

    def get_new_ioloop(self):
        return IOLoop.instance()

    @property
    def mongo_client(self):
        return self._app.settings['mongo_client']

    @property
    def default_db(self):
        return self.mongo_client[self.DATABASES[0]]

    def db_clear(self):
        @gen.engine
        def async_op(dname):
            yield motor.Op(self.mongo_client.drop_database, dname)
            self.stop()
        for dname in self.DATABASES:
            async_op(dname)
            self.wait()
