# -*- coding: utf-8 -*-
from base import BaseTest
from tornado.testing import gen_test
from example_app.models import SimpleModel


class TestDbOperations(BaseTest):

    @gen_test
    def test_save(self):
        secret = 'abbcc123'
        m = SimpleModel({"title": "Test model", "secret": secret})
        db = self.default_db
        yield m.save(db)
        m_from_db = yield SimpleModel.find_one(db, {"title": "Test model"})
        self.assertEqual(m.secret, m_from_db.secret)
