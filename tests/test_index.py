# coding: utf-8
from tornado.testing import gen_test
from example_app.models import SimpleModel, User, Event, Topic
from .base import BaseTest


class TestIndex(BaseTest):
	@gen_test
	def test_field_index(self):
		m = SimpleModel
		yield m.ensure_index(self.db)
		s = m({
			'title': 'Something',
			'secret': 'Nothing'
		})
		yield s.save(self.db)
		index_info = yield self.db[s._options.namespace].index_information()
		self.assertIn('title_1', index_info)

	@gen_test
	def test_fields_and_nested_index(self):
		u = yield User({'name': 'Test', 'age': 24}).save(self.db)
		m = Event
		yield m.ensure_index(self.db)
		e = m({
			'title': 'Something',
			'user': u
		})
		yield e.save(self.db)
		index_info = yield self.db[e._options.namespace].index_information()
		self.assertIn('title_1_user.name_1', index_info)

	@gen_test
	def test_field_unique_index(self):
		m = Topic
		yield m.ensure_index(self.db)
		t = m({
			'title': 'Something',
		})
		yield t.save(self.db)
		index_info = yield self.db[t._options.namespace].index_information()
		self.assertIn('title_1', index_info)
		self.assertTrue(index_info['title_1']['unique'])
