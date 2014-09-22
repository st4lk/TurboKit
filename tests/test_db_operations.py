# -*- coding: utf-8 -*-
import pymongo
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app.models import SimpleModel, Transaction, User


class TestDbOperations(BaseTest):

    @gen_test
    def test_save_and_get(self):
        secret = 'abbcc123'
        m = SimpleModel({"title": "Test model", "secret": secret})
        yield m.save(self.db)
        m_from_db = yield SimpleModel.objects.set_db(self.db).get({"title": "Test model"})
        self.assertEqual(m.secret, m_from_db.secret)

    @gen_test
    def test_all(self):
        models = yield self._create_models(self.db)
        model_secrets = set(map(lambda x: x.secret, models))
        result = yield SimpleModel.objects.set_db(self.db).all()
        model_db_secrets = set()
        for m in result:
            model_db_secrets.add(m.secret)
        self.assertEqual(model_secrets, model_db_secrets)

    @gen_test
    def test_filter(self):
        yield self._create_models(self.db)
        titles = ["1", "2"]
        result = yield SimpleModel.objects.set_db(self.db).filter(
            {"title": {"$in": titles}}).all()
        self.assertEqual(set(titles), set(map(lambda x: x.title, result)))

    @gen_test
    def test_sort(self):
        models = yield self._create_models(self.db, count=9)
        result = yield SimpleModel.objects.set_db(self.db).filter({})\
            .sort([("title", pymongo.DESCENDING), ("secret", pymongo.DESCENDING)]).all()
        for db_model, model in zip(result, sorted(models, key=lambda x: (-int(x.title), -int(x.secret)))):
            self.assertEqual(db_model, model)

    @gen_test
    def test_skip_limit(self):
        models = yield self._create_models(self.db, count=9)
        result = yield SimpleModel.objects.set_db(self.db).filter({})\
            .sort("secret", pymongo.ASCENDING).skip(2).limit(2).all()
        for db_model, model in zip(result, sorted(models, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)

    @gen_test
    def test_index_slice(self):
        models = yield self._create_models(self.db, count=9)
        result = yield SimpleModel.objects.set_db(self.db).filter({})\
            .sort("secret", pymongo.ASCENDING)[2:4]
        for db_model, model in zip(result, sorted(models, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)
        result = yield SimpleModel.objects.set_db(self.db).filter({})\
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


class TestGenericModelDbOperations(BaseTest):
    @gen_test
    def test_generic_model_save_and_get(self):
        # store SimpleModel in generic ref field
        sm = SimpleModel({"title": "Test model", "secret": 'abbcc123'})
        yield sm.save(self.db)
        t = Transaction(dict(title="yandex", item=sm))
        yield t.save(self.db)
        t_from_db = yield Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db.item, sm.pk)
        # try to save object, got from database
        t_from_db.title = "webmoney"
        yield t_from_db.save(self.db)
        t_from_db_1 = yield Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db_1.title, "webmoney")
        self.assertEqual(t_from_db_1.item, sm.pk)
        # replace value of generic ref field with another model class
        um = User(dict(name="Igor", age=15))
        yield um.save(self.db)
        t_from_db_1.item = um
        yield t_from_db_1.save(self.db)
        t_from_db_2 = yield Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db_2.pk, t.pk)
        self.assertEqual(t_from_db_2.item, um.pk)

    @gen_test
    def test_generic_all(self):
        # create models
        sm = SimpleModel({"title": "Test model", "secret": 'abbcc123'})
        yield sm.save(self.db)
        t1 = Transaction(dict(title="yandex", item=sm))
        yield t1.save(self.db)
        um = User(dict(name="Igor", age=15))
        yield um.save(self.db)
        t2 = Transaction(dict(title="webmoney", item=um))
        yield t2.save(self.db)
        # get them from db
        tns = yield Transaction.objects.set_db(self.db).all()
        tns.sort(key=lambda x: x.title)
        t1_db = tns[1]
        t2_db = tns[0]
        self.assertEqual(t1_db.title, t1.title)
        self.assertEqual(t1_db.item, sm.pk)
        self.assertEqual(t2_db.title, t2.title)
        self.assertEqual(t2_db.item, um.pk)
        # it is possible to save object, got from database
        yield t1_db.save(self.db)
