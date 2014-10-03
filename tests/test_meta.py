# coding: utf-8
from datetime import datetime
from tornado.testing import gen_test
from example_app.models import SimpleModel, SimpleSubModel, ActionBase, ActionSubclassed
from .base import BaseTest


class TestMeta(BaseTest):
	@gen_test
	def test_model_namespace_inheritance_with_options(self):
		self.assertEqual(SimpleModel._options.namespace, 'simple')
		self.assertEqual(SimpleSubModel._options.namespace, 'simplesub')

		sm = SimpleModel({
			'title': 'Something',
			'secret': 'Nothing',
		})
		ssm = SimpleSubModel({
			'extra': {
				'foo': 'moo',
				'because': 'poo',
			}
		})

		self.assertEqual(SimpleModel._options.namespace, sm._options.namespace)
		self.assertEqual(SimpleSubModel._options.namespace, ssm._options.namespace)

		sm_saved = yield sm.save(self.db)
		ssm_saved = yield ssm.save(self.db)

		self.assertEqual(SimpleModel._options.namespace, sm_saved._options.namespace)
		self.assertEqual(SimpleSubModel._options.namespace, ssm_saved._options.namespace)

		cn = yield self.db.collection_names()

		self.assertIn(SimpleModel._options.namespace, cn)
		self.assertIn(SimpleSubModel._options.namespace, cn)

		# print(sm_saved._id, sm_saved._id)

		sm = yield self.db[sm._options.namespace].find_one(sm_saved._id)
		ssm = yield self.db[ssm._options.namespace].find_one(ssm_saved._id)

		# print(sm, ssm)

		self.assertIsNotNone(sm)
		self.assertIsNotNone(ssm)

	@gen_test
	def test_model_namespace_inheritance_without_options(self):
		self.assertEqual(ActionBase._options.namespace, 'actionbase')
		self.assertEqual(ActionSubclassed._options.namespace, 'actionsubclassed')

		ab = ActionBase({
			'start_at': datetime.now()
		})
		asb = ActionSubclassed({
			'title': 'Something'
		})

		self.assertEqual(ActionBase._options.namespace, ab._options.namespace)
		self.assertEqual(ActionSubclassed._options.namespace, asb._options.namespace)

		ab_saved = yield ab.save(self.db)
		asb_saved = yield asb.save(self.db)

		self.assertEqual(ActionBase._options.namespace, ab_saved._options.namespace)
		self.assertEqual(ActionSubclassed._options.namespace, asb_saved._options.namespace)

		cn = yield self.db.collection_names()

		self.assertIn(ActionBase._options.namespace, cn)
		self.assertIn(ActionSubclassed._options.namespace, cn)

		# print(sm_saved._id, sm_saved._id)

		ab = yield self.db[ab._options.namespace].find_one(ab_saved._id)
		asb = yield self.db[asb._options.namespace].find_one(asb_saved._id)

		# print(sm, ssm)

		self.assertIsNotNone(ab)
		self.assertIsNotNone(asb)
