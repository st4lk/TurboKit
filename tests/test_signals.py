# -*- coding: utf-8 -*-
from base import BaseTest
from tornado.testing import gen_test
from tornado import gen
from example_app import models
from turbokit import signals


class TestSignals(BaseTest):
    def setUp(self):
        super(TestSignals, self).setUp()
        self.signal_called = False

    @gen_test
    def test_signal_pre_save(self):
        M = models.SimpleModel

        @gen.coroutine
        def do_before_save(sender, document):
            self.signal_called = True
            self.assertEqual(sender, M)
            self.assertEqual(document.secret, 'ninja')
            docs = yield M.objects.set_db(self.db).count()
            self.assertEqual(docs, 0)  # no document is saved yet

        signals.pre_save.connect(do_before_save, sender=M)

        sm = M(dict(secret='ninja'))
        yield sm.save(self.db)
        self.assertTrue(self.signal_called)

    @gen_test
    def test_signal_post_save(self):
        M = models.SimpleModel

        @gen.coroutine
        def do_after_save(sender, document):
            self.signal_called = True
            self.assertEqual(sender, M)
            self.assertEqual(document.secret, 'ninja')
            docs = yield M.objects.set_db(self.db).count()
            self.assertEqual(docs, 1)  # document is already saved

        signals.post_save.connect(do_after_save, sender=M)

        sm = M(dict(secret='ninja'))
        yield sm.save(self.db)
        self.assertTrue(self.signal_called)

    @gen_test
    def test_signal_pre_remove(self):
        M = models.SimpleModel

        @gen.coroutine
        def do_before_remove(sender, document):
            self.signal_called = True
            self.assertEqual(sender, M)
            self.assertEqual(document.secret, 'ninja')
            docs = yield M.objects.set_db(self.db).count()
            self.assertEqual(docs, 1)  # document is not removed yet

        signals.pre_remove.connect(do_before_remove, sender=M)

        sm = M(dict(secret='ninja'))
        yield sm.save(self.db)
        self.assertFalse(self.signal_called)
        yield sm.remove(self.db)
        self.assertTrue(self.signal_called)

    @gen_test
    def test_signal_post_remove(self):
        M = models.SimpleModel

        @gen.coroutine
        def do_after_remove(sender, document):
            self.signal_called = True
            self.assertEqual(sender, M)
            self.assertEqual(document.secret, 'ninja')
            docs = yield M.objects.set_db(self.db).count()
            self.assertEqual(docs, 0)  # document is already removed

        signals.post_remove.connect(do_after_remove, sender=M)

        sm = M(dict(secret='ninja'))
        yield sm.save(self.db)
        self.assertFalse(self.signal_called)
        yield sm.remove(self.db)
        self.assertTrue(self.signal_called)
