# -*- coding: utf-8 -*-
"""
Base code is taken from https://github.com/wsantos/motorm
"""
import logging
from bson.objectid import ObjectId
# from bson.son import SON
from tornado import gen
from schematics.models import Model as SchematicsModel
# from schematics.types.compound import ListType
from pymongo.errors import OperationFailure
from .cursors import AsyncManagerCursor, PrefetchRelatedMixin
from .types import NULLIFY, CASCADE, DENY, PULL
from .errors import OperationError

l = logging.getLogger(__name__)


class AsyncManager(PrefetchRelatedMixin):

    def __init__(self, cls, collection, db=None, **kwargs):
        super(AsyncManager, self).__init__(cls, collection, db=db, **kwargs)
        self.collection = collection
        self.cls = cls
        self.db = db

    def set_db(self, db):
        """
        Create another instance of AsyncManager, so database won't be shared
        between all models. It is thread safe.
        """
        return AsyncManager(self.cls, self.collection, db)

    @gen.coroutine
    def get(self, query, return_raw=False):
        # TODO: add reconnects here and in other methods
        query = self.process_query(query)
        response = yield self.db[self.collection].find_one(query)
        if return_raw:
            result = response
        else:
            m = self.cls(response, from_mongo=True)
            results_with_related = yield self.fetch_related_objects([m])
            result = results_with_related[0]
        raise gen.Return(result)

    @gen.coroutine
    def update(self, query, raw_data, upsert=False, multi=False):
        query = self.process_query(query)
        result = yield self.db[self.collection].update(query, raw_data,
            upsert=upsert, multi=multi)
        if result['ok'] != 1:
            # TODO how to catch this exception?
            raise OperationFailure(result, code=result['ok'])
        raise gen.Return(result)

    @gen.coroutine
    def remove(self, query, docs_tobe_deleted=None, **kwargs):
        query = self.process_query(query)
        if not docs_tobe_deleted:
            # TODO use next or fetch with skip and limit
            docs_tobe_deleted = yield self.filter(query).all()
        for doc in docs_tobe_deleted:
            delete_rules = getattr(doc._options, 'delete_rules', {})
            # check DENY rule first. If even one deny rule is matched,
            # deny entire remove action
            for rule_entry in delete_rules:
                parent_doc_cls, parent_field_name = rule_entry
                rule = delete_rules[rule_entry]
                if rule == DENY:
                    cnt = yield parent_doc_cls.objects.set_db(self.db).filter(
                        {parent_field_name: doc.pk}).count()
                    if cnt > 0:
                        raise OperationError(
                            "Could not delete document ({0}.{1} refers to it)"
                            .format(parent_doc_cls.__name__, parent_field_name))
            # now check other delete rules
            for rule_entry in delete_rules:
                parent_doc_cls, parent_field_name = rule_entry
                rule = delete_rules[rule_entry]
                if rule == NULLIFY:
                    parent_doc_cls.objects.set_db(self.db).update(
                        {parent_field_name: doc.pk},
                        {"$unset": {parent_field_name: ""}}, multi=True)
                elif rule == CASCADE:
                    parent_doc_cls.objects.set_db(self.db).remove(
                        {parent_field_name: doc.pk})

        result = yield self.db[self.collection].remove(query, **kwargs)
        if result['ok'] != 1:
            # TODO how to catch this exception?
            raise OperationFailure(result, code=result['ok'])
        raise gen.Return(result)

    @gen.coroutine
    def all(self):
        cursor = self.db[self.collection].find({})
        results = yield AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related).all()
        raise gen.Return(results)

    @gen.coroutine
    def count(self, with_limit_and_skip=True):
        cursor = self.db[self.collection].find({})
        result = yield AsyncManagerCursor(self.cls, cursor, self.db).count(
            with_limit_and_skip=with_limit_and_skip)
        raise gen.Return(result)

    @gen.coroutine
    def aggregate(self, pipeline, **kwargs):
        """
        With server version >= 2.5.1, pass cursor={} to retrieve unlimited
        aggregation results with a CommandCursor
        """
        result = yield self.db[self.collection].aggregate(pipeline, **kwargs)
        if 'cursor' not in kwargs:
            if result['ok'] != 1:
                # TODO how to catch this exception?
                raise OperationFailure(result, code=result['ok'])
            result = result['result']
        raise gen.Return(result)

    def prefetch_related(self, *args):
        return AsyncManager(self.cls, self.collection, self.db,
            prefetch_related=self._prefetch_related | set(args))

    def filter(self, query):
        query = self.process_query(query)
        cursor = self.db[self.collection].find(query)
        return AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related)

    def process_query(self, query):
        if 'id' in query:
            _id = query.pop('id')
            if isinstance(_id, SchematicsModel):
                _id = _id.pk
            if not isinstance(_id, ObjectId) and not isinstance(_id, dict):
                query['_id'] = ObjectId(_id)
            else:
                query['_id'] = _id
        return query
