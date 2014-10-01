# -*- coding: utf-8 -*-
import logging
import pymongo
from datetime import datetime, timedelta
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app import models
from turbokit.errors import OperationError

l = logging.getLogger(__name__)


class TestDbOperations(BaseTest):

    @gen_test
    def test_save_and_get(self):
        secret = 'abbcc123'
        m = models.SimpleModel({"title": "Test model", "secret": secret})
        yield m.save(self.db)
        m_from_db = yield models.SimpleModel.objects.set_db(self.db).get({"title": "Test model"})
        self.assertEqual(m.secret, m_from_db.secret)

    @gen_test
    def test_get_none(self):
        m_from_db = yield models.SimpleModel.objects.set_db(self.db).get({"title": "Test model"})
        self.assertTrue(m_from_db is None)

    @gen_test
    def test_all(self):
        mdls = yield self._create_models(self.db)
        model_secrets = set(map(lambda x: x.secret, mdls))
        result = yield models.SimpleModel.objects.set_db(self.db).all()
        model_db_secrets = set()
        for m in result:
            model_db_secrets.add(m.secret)
        self.assertEqual(model_secrets, model_db_secrets)

    @gen_test
    def test_filter(self):
        yield self._create_models(self.db)
        titles = ["1", "2"]
        result = yield models.SimpleModel.objects.set_db(self.db).filter(
            {"title": {"$in": titles}}).all()
        self.assertEqual(set(titles), set(map(lambda x: x.title, result)))

    @gen_test
    def test_sort(self):
        mdls = yield self._create_models(self.db, count=9)
        result = yield models.SimpleModel.objects.set_db(self.db).filter({})\
            .sort([("title", pymongo.DESCENDING), ("secret", pymongo.DESCENDING)]).all()
        for db_model, model in zip(result, sorted(mdls, key=lambda x: (-int(x.title), -int(x.secret)))):
            self.assertEqual(db_model, model)

    @gen_test
    def test_skip_limit(self):
        mdls = yield self._create_models(self.db, count=9)
        result = yield models.SimpleModel.objects.set_db(self.db).filter({})\
            .sort("secret", pymongo.ASCENDING).skip(2).limit(2).all()
        for db_model, model in zip(result, sorted(mdls, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)

    @gen_test
    def test_index_slice(self):
        mdls = yield self._create_models(self.db, count=9)
        result = yield models.SimpleModel.objects.set_db(self.db).filter({})\
            .sort("secret", pymongo.ASCENDING)[2:4]
        for db_model, model in zip(result, sorted(mdls, key=lambda x: x.secret)[2:4]):
            self.assertEqual(db_model, model)
        result = yield models.SimpleModel.objects.set_db(self.db).filter({})\
            .sort("secret", pymongo.ASCENDING)[2]
        self.assertEqual(result, sorted(mdls, key=lambda x: x.secret)[2])

    @gen_test
    def test_serializable_fields(self):
        plan = models.Plan()
        self.assertTrue(plan.is_expired)
        yield plan.save(self.db)
        plan_db_raw = yield models.Plan.objects.set_db(self.db).get({'id': plan.pk},
            return_raw=True)
        self.assertFalse('is_expired' in plan_db_raw)
        plan_db = yield models.Plan.objects.set_db(self.db).get({'id': plan.pk})
        plan_db.ends_at = datetime.now() + timedelta(days=1)
        self.assertFalse(plan_db.is_expired)
        plan_db.save(self.db)
        plan_db = yield models.Plan.objects.set_db(self.db).get({'id': plan.pk})
        self.assertFalse(plan_db.is_expired)

    @gen_test
    def test_count(self):
        yield self._create_models(self.db, count=9)
        # count all
        count = yield models.SimpleModel.objects.set_db(self.db).count()
        self.assertEqual(count, 9)
        # count filter
        count = yield models.SimpleModel.objects.set_db(self.db).filter(
            {"secret": {"$in": ['1', '2']}}).count()
        self.assertEqual(count, 2)
        # count skip, limit
        count = yield models.SimpleModel.objects.set_db(self.db).filter({})\
            .skip(2).limit(5).count()
        self.assertEqual(count, 5)

    @gen_test
    def test_update(self):
        # TODO
        pass

    @gen_test
    def test_insert(self):
        M = models.SimpleModel
        m = M(dict(title='t1', secret='s1'))
        m_id = yield M.objects.set_db(self.db).insert(m)
        m_from_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(m_from_db), 1)
        self.assertEqual(m_from_db[0].pk, m_id)
        self.assertEqual(m_from_db[0].secret, 's1')

    @gen_test
    def test_bulk_insert(self):
        M = models.SimpleModel
        mdls = []
        for i in range(5):
            mdls.append(M(dict(title='t{0}'.format(i), secret='s{0}'.format(i))))
        m_ids = yield M.objects.set_db(self.db).insert(mdls)
        mdls_from_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(mdls_from_db), 5)
        for m, m_db in zip(mdls, sorted(mdls_from_db, key=lambda d: d.title)):
            self.assertEqual(m.secret, m_db.secret)
        for m_id, m_db in zip(sorted(m_ids), sorted(mdls_from_db, key=lambda d: d.pk)):
            self.assertEqual(m_id, m_db.pk)

    @gen.coroutine
    def _create_models(self, db, count=5):
        mdls = []
        for i in range(count):
            m = models.SimpleModel({"title": str(i / 2 + i % 2), "secret": str(i)})
            yield m.save(db)
            mdls.append(m)
        raise gen.Return(mdls)


class TestGenericModelDbOperations(BaseTest):
    @gen_test
    def test_generic_model_save_and_get(self):
        # store models.SimpleModel in generic ref field
        sm = models.SimpleModel({"title": "Test model", "secret": 'abbcc123'})
        yield sm.save(self.db)
        t = models.Transaction(dict(title="yandex", item=sm))
        yield t.save(self.db)
        t_from_db = yield models.Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db.item, sm.pk)
        # try to save object, got from database
        t_from_db.title = "webmoney"
        yield t_from_db.save(self.db)
        t_from_db_1 = yield models.Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db_1.title, "webmoney")
        self.assertEqual(t_from_db_1.item, sm.pk)
        # replace value of generic ref field with another model class
        um = models.User(dict(name="Igor", age=15))
        yield um.save(self.db)
        t_from_db_1.item = um
        yield t_from_db_1.save(self.db)
        t_from_db_2 = yield models.Transaction.objects.set_db(self.db).get({"id": t.pk})
        self.assertEqual(t_from_db_2.pk, t.pk)
        self.assertEqual(t_from_db_2.item, um.pk)

    @gen_test
    def test_generic_all(self):
        # create models
        sm = models.SimpleModel({"title": "Test model", "secret": 'abbcc123'})
        yield sm.save(self.db)
        t1 = models.Transaction(dict(title="yandex", item=sm))
        yield t1.save(self.db)
        um = models.User(dict(name="Igor", age=15))
        yield um.save(self.db)
        t2 = models.Transaction(dict(title="webmoney", item=um))
        yield t2.save(self.db)
        # get them from db
        tns = yield models.Transaction.objects.set_db(self.db).all()
        tns.sort(key=lambda x: x.title)
        t1_db = tns[1]
        t2_db = tns[0]
        self.assertEqual(t1_db.title, t1.title)
        self.assertEqual(t1_db.item, sm.pk)
        self.assertEqual(t2_db.title, t2.title)
        self.assertEqual(t2_db.item, um.pk)
        # it is possible to save object, got from database
        yield t1_db.save(self.db)

    @gen_test
    def test_remove_single_object(self):
        sm = models.SimpleModel()
        yield sm.save(self.db)
        yield sm.remove(self.db)
        sm_db = yield models.SimpleModel.objects.set_db(self.db).all()
        self.assertEqual(len(sm_db), 0)

    @gen_test
    def test_remove_query(self):
        for i in range(3):
            sm = models.SimpleModel(dict(title=str(i)))
            yield sm.save(self.db)
        yield models.SimpleModel.objects.set_db(self.db).remove(
            {"title": {"$in": ['0', '1']}})
        sm_db = yield models.SimpleModel.objects.set_db(self.db).all()
        self.assertEqual(len(sm_db), 1)
        self.assertEqual(sm.pk, sm_db[0].pk)


class TestDymanicDbOperations(BaseTest):
    @gen_test
    def test_dynamic_save_and_get(self):
        # store primitive
        p = models.Page(dict(title="first", content=123))
        yield p.save(self.db)
        p_from_db = yield models.Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store list
        p.content = [1, 2, 3]
        yield p.save(self.db)
        p_from_db = yield models.Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store dict
        p.content = {'a': 1, 'b': 'val'}
        yield p.save(self.db)
        p_from_db = yield models.Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(p.content, p_from_db.content)
        # store embedded model
        nm = models.NestedModel(dict(type_string="good", type_int=15))
        p.content = nm
        yield p.save(self.db)
        p_from_db = yield models.Page.objects.set_db(self.db).get({"id": p.pk})
        self.assertEqual(nm, p_from_db.content)

    @gen_test
    def test_dynamic_list_save_and_get(self):
        # test primitive
        menu = [1, 'one', {"two": "pk"}]
        b = models.Brand(dict(title="main", menu=menu))
        yield b.save(self.db)
        b_from_db = yield models.Brand.objects.set_db(self.db).get({'id': b.pk})
        self.assertEqual(b_from_db.menu, menu)
        # test embedded model
        nm = models.NestedModel(dict(type_string="good", type_int=15))
        menu.append(nm)
        b.menu = menu
        yield b.save(self.db)
        b_from_db = yield models.Brand.objects.set_db(self.db).get({'id': b.pk})
        self.assertEqual(b_from_db.menu, menu)

    @gen_test
    def test_dynamic_list_all(self):
        # test primitive
        menu1 = [1, 'one', {"two": "pk"}]
        b1 = models.Brand(dict(title="main", menu=menu1))
        yield b1.save(self.db)
        nm1 = models.NestedModel(dict(type_string="good", type_int=15))
        nm2 = models.NestedModel(dict(type_string="bad", type_int=42))
        menu2 = list(menu1) + [nm1, nm2]
        b2 = models.Brand(dict(title="zoop", menu=menu2))
        yield b2.save(self.db)
        bs_from_db = yield models.Brand.objects.set_db(self.db).all()
        bs_from_db.sort(key=lambda x: x.title)
        b1_from_db = bs_from_db[0]
        b2_from_db = bs_from_db[1]
        self.assertEqual(b1_from_db.menu, menu1)
        self.assertEqual(b2_from_db.menu, menu2)


class TestReverseDeleteRulesSingleField(BaseTest):

    @gen_test
    def test_single_do_nothing(self):
        M = models.ParentA
        child = yield self._create_child()
        parent = M(dict(child=child))
        yield parent.save(self.db)
        yield child.remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 1)
        self.assertEqual(parents_db[0].child, child.pk)

    @gen_test
    def test_single_nullify(self):
        M = models.ParentB
        child = yield self._create_child()
        parent = M(dict(child=child))
        yield parent.save(self.db)
        yield child.remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 1)
        self.assertEqual(parents_db[0].child, None)

    @gen_test
    def test_single_cascade(self):
        M = models.ParentC
        child = yield self._create_child()
        for i in range(2):
            parent = M(dict(child=child))
            yield parent.save(self.db)
        yield child.remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 0)

    @gen_test
    def test_single_deny(self):
        M = models.ParentD
        child = yield self._create_child()
        parent = M(dict(child=child))
        yield parent.save(self.db)
        with self.assertRaises(OperationError):
            yield child.remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 1)

    @gen.coroutine
    def _create_child(self):
        child = models.ChildA()
        yield child.save(self.db)
        raise gen.Return(child)


class TestReverseDeleteRulesListField(BaseTest):

    @gen.coroutine
    def _check_do_nothing(self, from_queryset=False):
        M = models.ParentE
        childs, parent = yield self._create_models(M)
        if from_queryset:
            yield models.ChildA.objects.set_db(self.db).remove(
                {"id": {"$in": map(lambda x: x.pk, childs)}})
        else:
            yield childs[0].remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 1)
        self.assertEqual(len(parents_db[0].childs), len(childs))
        for c_db, c_was in zip(parents_db[0].childs, childs):
            self.assertEqual(c_db, c_was.pk)

    @gen.coroutine
    def _check_nullify(self, from_queryset=False):
        M = models.ParentF
        childs, parent = yield self._create_models(M)
        if from_queryset:
            childs_to_delete = childs[:2]
            yield models.ChildA.objects.set_db(self.db).remove(
                {"id": {"$in": map(lambda x: x.pk, childs_to_delete)}})
        else:
            childs_to_delete = childs[:1]
            yield childs_to_delete[0].remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        self.assertEqual(len(parents_db), 1)
        self.assertEqual(parents_db[0].childs, None)

    @gen.coroutine
    def _check_cascade(self, from_queryset=False):
        M = models.ParentG
        parents_db = yield M.objects.set_db(self.db).all()
        l.info("Parents in database before test: {0}".format(
            map(lambda p: p.pk, parents_db)))
        if from_queryset:
            childs1, p1 = yield self._create_models(M)
            l.info("Saved parent1 {0}".format(p1.pk))
            l.info("Saved childs1 {0}".format(map(lambda chd: str(chd.pk), childs1)))
            childs2, p2 = yield self._create_models(M)
            l.info("Saved parent2 {0}".format(p2.pk))
            l.info("Saved childs2 {0}".format(map(lambda chd: str(chd.pk), childs2)))
            childs_exist, parent_exist = yield self._create_models(M)
            l.info("Saved parent3 {0}".format(parent_exist.pk))
            l.info("Saved childs3 {0}".format(map(lambda chd: str(chd.pk), childs_exist)))
            childs_to_delete = childs1[0:2] + childs2[:1]
            l.info("childs_to_delete {0}".format(map(lambda chd: str(chd.pk), childs_to_delete)))
            yield models.ChildA.objects.set_db(self.db).remove(
                {"id": {"$in": map(lambda x: x.pk, childs_to_delete)}})
        else:
            childs1, p1 = yield self._create_models(M)
            l.info("Saved parent1 {0}".format(p1.pk))
            l.info("Saved childs1 {0}".format(map(lambda chd: str(chd.pk), childs1)))
            childs_exist, parent_exist = yield self._create_models(M)
            l.info("Saved parent2 {0}".format(parent_exist.pk))
            l.info("Saved childs2 {0}".format(map(lambda chd: str(chd.pk), childs_exist)))
            childs_to_delete = childs1[:1]
            l.info("childs_to_delete {0}".format(map(lambda chd: str(chd.pk), childs_to_delete)))
            yield childs_to_delete[0].remove(self.db)
        parents_db = yield M.objects.set_db(self.db).all()
        l.info("Parents in database: {0}".format(map(lambda p: p.pk, parents_db)))
        self.assertEqual(len(parents_db), 1)
        self.assertEqual(parents_db[0].pk, parent_exist.pk)
        self.assertEqual(len(parents_db[0].childs), len(childs_exist))
        self.assertEqual(map(lambda x: x.pk, childs_exist),
            parents_db[0].childs)

    @gen.coroutine
    def _check_deny(self, from_queryset=False):
        M1 = models.ParentE
        M2 = models.ParentH
        childs1, parent1 = yield self._create_models(M1)
        childs2, parent2 = yield self._create_models(M2)
        if from_queryset:
            childs_to_delete = childs1[:1] + childs2[:1]
            with self.assertRaises(OperationError):
                yield models.ChildA.objects.set_db(self.db).remove(
                    {"id": {"$in": map(lambda x: x.pk, childs_to_delete)}})
        else:
            childs_to_delete = childs2[:1]
            with self.assertRaises(OperationError):
                yield childs_to_delete[0].remove(self.db)
        parents1_db = yield M1.objects.set_db(self.db).all()
        parents2_db = yield M2.objects.set_db(self.db).all()
        self.assertEqual(len(parents1_db), 1)
        self.assertEqual(len(parents2_db), 1)
        for p_db, clds in [[parents1_db[0], childs1], [parents2_db[0], childs2]]:
            self.assertEqual(p_db.childs, map(lambda x: x.pk, clds))

    @gen.coroutine
    def _check_pull(self, from_queryset=False):
        M1 = models.ParentE
        M2 = models.ParentI
        childs1, parent1 = yield self._create_models(M1)
        childs2, parent2 = yield self._create_models(M2)
        childs3, parent3 = yield self._create_models(M2)
        yield parent3.update(self.db, {"$push": {"childs": childs2[0].pk}}, raw=True)
        if from_queryset:
            childs_to_delete = childs1[:1] + childs2[:1]
            yield models.ChildA.objects.set_db(self.db).remove(
                {"id": {"$in": map(lambda x: x.pk, childs_to_delete)}})
        else:
            childs_to_delete = childs1[:1] + childs2[:1]
            yield childs_to_delete[0].remove(self.db)
            yield childs_to_delete[1].remove(self.db)
        parents1_db = yield M1.objects.set_db(self.db).all()
        parents2_db = yield M2.objects.set_db(self.db).all()
        self.assertEqual(len(parents1_db), 1)
        self.assertEqual(len(parents2_db), 2)
        for p_db, clds in [
                [parents1_db[0], childs1],
                [parents2_db[0], childs2[1:]],
                [parents2_db[1], childs3]]:
            self.assertEqual(p_db.childs, map(lambda x: x.pk, clds))

    @gen.coroutine
    def _create_child(self, model=None):
        model = model or models.ChildA
        child = model()
        yield child.save(self.db)
        raise gen.Return(child)

    @gen.coroutine
    def _create_models(self, parent_class):
        childs = []
        for i in range(3):
            child = yield self._create_child()
            childs.append(child)
        parent = parent_class(dict(childs=childs))
        yield parent.save(self.db)
        raise gen.Return((childs, parent))

    @gen_test
    def test_list_do_nothing_from_object(self):
        yield self._check_do_nothing(from_queryset=False)

    @gen_test
    def test_list_do_nothing_from_queryset(self):
        yield self._check_do_nothing(from_queryset=True)

    @gen_test
    def test_list_nullify_from_object(self):
        yield self._check_nullify(from_queryset=False)

    @gen_test
    def test_list_nullify_from_queryset(self):
        yield self._check_nullify(from_queryset=True)

    @gen_test
    def test_list_cascade_from_object(self):
        yield self._check_cascade(from_queryset=False)

    @gen_test
    def test_list_cascade_from_queryset(self):
        yield self._check_cascade(from_queryset=True)

    @gen_test
    def test_list_deny_from_object(self):
        yield self._check_deny(from_queryset=False)

    @gen_test
    def test_list_deny_from_queryset(self):
        yield self._check_deny(from_queryset=True)

    @gen_test
    def test_list_pull_from_object(self):
        yield self._check_pull(from_queryset=False)

    @gen_test
    def test_list_pull_from_queryset(self):
        yield self._check_pull(from_queryset=True)

    @gen_test
    def test_delete_rule_inheritance_own_field(self):
        for M in [models.ParentK, models.ParentSublcassed]:
            parent, guru, friends = yield self._create_model_with_mixin(M)
            yield parent.save(self.db)
            yield guru.remove(self.db)
            p_db = yield M.objects.set_db(self.db).get({'id': parent.pk})
            self.assertEqual(p_db.guru, None)

    @gen_test
    def test_delete_rule_inheritance_field(self):
        for M in [models.ParentK, models.ParentSublcassed]:
            parent, guru, friends = yield self._create_model_with_mixin(M)
            yield parent.save(self.db)
            yield friends[0].remove(self.db)
            p_db = yield M.objects.set_db(self.db).get({'id': parent.pk})
            self.assertEqual(p_db.guru, guru.pk)
            self.assertEqual(len(p_db.friends), 1)
            self.assertEqual(p_db.friends[0], friends[1].pk)

    @gen.coroutine
    def _create_model_with_mixin(self, model):
        friends = []
        for i in range(2):
            friend = yield self._create_child(models.ChildB)
            friends.append(friend)
        guru = yield self._create_child(models.ChildC)
        parent = model(dict(friends=friends, guru=guru))
        raise gen.Return((parent, guru, friends))
