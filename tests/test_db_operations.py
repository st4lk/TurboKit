# -*- coding: utf-8 -*-
import pymongo
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app.models import SimpleModel


class TestDbOperations(BaseTest):

    @gen_test
    def test_save(self):
        secret = 'abbcc123'
        m = SimpleModel({"title": "Test model", "secret": secret})
        db = self.default_db
        yield m.save(db)
        m_from_db = yield SimpleModel.objects.set_db(db).get({"title": "Test model"})
        self.assertEqual(m.secret, m_from_db.secret)

    @gen_test
    def test_all(self):
        db = self.default_db
        models = yield self._create_models(db)
        model_secrets = set(map(lambda x: x.secret, models))
        result = yield SimpleModel.objects.set_db(db).all()
        model_db_secrets = set()
        for m in result:
            model_db_secrets.add(m.secret)
        self.assertEqual(model_secrets, model_db_secrets)

    @gen_test
    def test_filter(self):
        db = self.default_db
        yield self._create_models(db)
        titles = ["1", "2"]
        result = yield SimpleModel.objects.set_db(db).filter(
            {"title": {"$in": titles}}).all()
        self.assertEqual(set(titles), set(map(lambda x: x.title, result)))

    @gen_test
    def test_sort(self):
        db = self.default_db
        models = yield self._create_models(db, count=9)
        result = yield SimpleModel.objects.set_db(db).filter({})\
            .sort("title", pymongo.DESCENDING).all()
        for db_model, model in zip(result, sorted(models, key=lambda x: (-int(x.title), int(x.secret)))):
            self.assertEqual(db_model, model)

    @gen_test
    def test_skip_limit(self):
        db = self.default_db
        models = yield self._create_models(db, count=9)
        result = yield SimpleModel.objects.set_db(db).filter({})\
            .sort("secret", pymongo.ASCENDING).skip(2).limit(2).all()
        for db_model, model in zip(result, sorted(models, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)

    @gen_test
    def test_index_slice(self):
        db = self.default_db
        models = yield self._create_models(db, count=9)
        result = yield SimpleModel.objects.set_db(db).filter({})\
            .sort("secret", pymongo.ASCENDING)[2:4]
        for db_model, model in zip(result, sorted(models, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)
        result = yield SimpleModel.objects.set_db(db).filter({})\
            .sort("secret", pymongo.ASCENDING)[2]
        self.assertEqual(result, sorted(models, key=lambda x: x.secret)[2])

    @gen.coroutine
    def _create_models(self, db, count=5):
        models = []
        for i in range(count):
            m = SimpleModel({"title": str(i / 2 + i % 2), "secret": str(i)})
            yield m.save(db)
            models.append(m)
        raise gen.Return(models)
