# -*- coding: utf-8 -*-
import motor
from tornado.testing import AsyncHTTPTestCase
from tornado.ioloop import IOLoop
from tornado import gen
from example_app.app import AppODM


app = AppODM()
reverse_url = app.reverse_url
mongo_client = app.settings['mongo_client']
db_name = 'odm_test'


class BaseTest(AsyncHTTPTestCase):
    DATABASES = [db_name, ]

    def setUp(self):
        super(BaseTest, self).setUp()
        self.reverse_url = reverse_url
        self.db_clear()

    def tearDown(self):
        super(BaseTest, self).tearDown()
        self.db_clear()

    def get_app(self):
        return app

    def get_new_ioloop(self):
        return IOLoop.instance()

    def db_clear(self):
        @gen.engine
        def async_op(dname):
            yield motor.Op(mongo_client.drop_database, dname)
            self.stop()
        for dname in self.DATABASES:
            async_op(dname)
            self.wait()
