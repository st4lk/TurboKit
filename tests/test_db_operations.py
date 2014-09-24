# -*- coding: utf-8 -*-
import pymongo
from datetime import datetime, timedelta
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app.models import (SimpleModel, Transaction, User, Page,
    NestedModel, Brand, Plan)


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

    @gen_test
    def test_serializable_fields(self):
        plan = Plan()
        self.assertTrue(plan.is_expired)
        yield plan.save(self.db)
        plan_db_raw = yield Plan.objects.set_db(self.db).get({'id': plan.pk},
            return_raw=True)
        self.assertFalse('is_expired' in plan_db_raw)
        plan_db = yield Plan.objects.set_db(self.db).get({'id': plan.pk})
        plan_db.ends_at = datetime.now() + timedelta(days=1)
        self.assertFalse(plan_db.is_expired)
        plan_db.save(self.db)
        plan_db = yield Plan.objects.set_db(self.db).get({'id': plan.pk})
        self.assertFalse(plan_db.is_expired)

    @gen_test
    def test_count(self):
        yield self._create_models(self.db, count=9)
        # count all
        count = yield SimpleModel.objects.set_db(self.db).count()
        self.assertEqual(count, 9)
        # count filter
        count = yield SimpleModel.objects.set_db(self.db).filter(
            {"secret": {"$in": ['1', '2']}}).count()
        self.assertEqual(count, 2)
        # count skip, limit
        count = yield SimpleModel.objects.set_db(self.db).filter({})\
            .skip(2).limit(5).count()
        self.assertEqual(count, 5)

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


class TestDymanicDbOperations(BaseTest):
    @gen_test
    def test_dynamic_save_and_get(self):
        # store primitive
        p = Page(dict(title="first", content=123))
        yield p.save(self.db)
        p_from_db = yield Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store list
        p.content = [1, 2, 3]
        yield p.save(self.db)
        p_from_db = yield Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store dict
        p.content = {'a': 1, 'b': 'val'}
        yield p.save(self.db)
        p_from_db = yield Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store embedded model
        nm = NestedModel(dict(type_string="good", type_int=15))
        p.content = nm
        yield p.save(self.db)
        p_from_db = yield Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(nm, p_from_db.content)

    @gen_test
    def test_dynamic_list_save_and_get(self):
        # test primitive
        menu = [1, 'one', {"two": "pk"}]
        b = Brand(dict(title="main", menu=menu))
        yield b.save(self.db)
        b_from_db = yield Brand.objects.set_db(self.db).get({'id': b.pk})
        self.assertEqual(b_from_db.menu, menu)
        # test embedded model
        nm = NestedModel(dict(type_string="good", type_int=15))
        menu.append(nm)
        b.menu = menu
        yield b.save(self.db)
        b_from_db = yield Brand.objects.set_db(self.db).get({'id': b.pk})
        self.assertEqual(b_from_db.menu, menu)

    @gen_test
    def test_dynamic_list_all(self):
        # test primitive
        menu1 = [1, 'one', {"two": "pk"}]
        b1 = Brand(dict(title="main", menu=menu1))
        yield b1.save(self.db)
        nm1 = NestedModel(dict(type_string="good", type_int=15))
        nm2 = NestedModel(dict(type_string="bad", type_int=42))
        menu2 = list(menu1) + [nm1, nm2]
        b2 = Brand(dict(title="zoop", menu=menu2))
        yield b2.save(self.db)
        bs_from_db = yield Brand.objects.set_db(self.db).all()
        bs_from_db.sort(key=lambda x: x.title)
        b1_from_db = bs_from_db[0]
        b2_from_db = bs_from_db[1]
        self.assertEqual(b1_from_db.menu, menu1)
        self.assertEqual(b2_from_db.menu, menu2)
