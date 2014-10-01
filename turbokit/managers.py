# -*- coding: utf-8 -*-
"""
Base code is taken from https://github.com/wsantos/motorm
"""
import logging
from bson.objectid import ObjectId
from tornado import gen
from schematics.models import Model as SchematicsModel
from pymongo.errors import OperationFailure
from .cursors import AsyncManagerCursor, PrefetchRelatedMixin
from .types import NULLIFY, CASCADE, DENY, PULL
from .errors import OperationError
from .signals import pre_remove, post_remove

l = logging.getLogger(__name__)


class AsyncManager(PrefetchRelatedMixin):

    def __init__(self, cls, collection, db=None, fields=None, **kwargs):
        super(AsyncManager, self).__init__(cls, collection, db=db, **kwargs)
        self.collection = collection
        self.cls = cls
        self.db = db
        if fields is None:
            fields = {}
        self.fields = fields

    def exclude(self, *fields):
        # TODO exclude also self.cls._serializables
        # TODO respect prefeched model fields
        _fields = self.fields.copy()
        exclude_fields = dict(map(lambda x: (x, False), fields))
        if not _fields:
            _fields = exclude_fields
        else:
            if _fields.itervalues().next():
                # _fields contains all True values, convert it to False
                fields_converted = set(self.cls._fields) - set(_fields)
                _fields = dict(map(lambda x: (x, False), fields_converted))
                _fields.update(exclude_fields)
                _fields.pop('_id', None)  # TODO handle id exclusion
            else:
                # _fields contains all False values, just update
                _fields.update(exclude_fields)
        return AsyncManager(self.cls, self.collection, self.db, _fields)

    def only(self, *fields):
        # TODO only also self.cls._serializables
        # TODO respect prefeched model fields
        _fields = self.fields.copy()
        only_fields = dict(map(lambda x: (x, True), fields))
        if not _fields:
            _fields = only_fields
        else:
            if _fields.itervalues().next():
                # _fields contains all True values, just update
                _fields.update(_fields)
            else:
                # _fields contains all False values, convert it to True
                for exclude_field in _fields:
                    only_fields.pop(exclude_field, None)
                _fields = only_fields
        return AsyncManager(self.cls, self.collection, self.db, _fields)

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
        params = self.get_find_extra_params()
        response = yield self.db[self.collection].find_one(query, **params)
        if return_raw:
            result = response
        elif response:
            m = self.cls(response, from_mongo=True)
            results_with_related = yield self.fetch_related_objects([m])
            result = results_with_related[0]
        else:
            result = None
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
    def insert(self, doc_or_docs, load_bulk=False, **kwargs):
        """bulk insert documents

        :param doc_or_docs: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances

        By default returns  ObjectIds, set ``load_bulk`` to True to
        return document instances.
        """
        return_one = False
        if isinstance(doc_or_docs, (list, tuple)):
            docs = doc_or_docs
        else:
            return_one = True
            docs = [doc_or_docs]
        raw = []
        for doc in docs:
            if not isinstance(doc, self.cls):
                raise OperationError(u"Some documents inserted aren't "
                    "instances of {0}".self.cls)
            raw.append(doc.to_mongo())
        ids = yield self.db[self.collection].insert(raw, **kwargs)
        if not load_bulk:
            result = return_one and ids[0] or ids
        else:
            NotImplementedError()  # TODO
        raise gen.Return(result)

    @gen.coroutine
    def remove(self, query, docs_tobe_deleted=None, **kwargs):
        query = self.process_query(query)
        if not docs_tobe_deleted:
            # TODO use next or fetch with skip and limit
            docs_tobe_deleted = yield self.filter(query).all()
        for doc in docs_tobe_deleted:
            yield pre_remove.send(doc.__class__, document=doc)
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
                l.debug('processing delete rule {0} for {1}'.format(rule, parent_doc_cls.__name__))
                if rule == NULLIFY:
                    parent_doc_cls.objects.set_db(self.db).update(
                        {parent_field_name: doc.pk},
                        {"$unset": {parent_field_name: ""}}, multi=True)
                elif rule == CASCADE:
                    parent_doc_cls.objects.set_db(self.db).remove(
                        {parent_field_name: doc.pk})
                elif rule == PULL:
                    parent_doc_cls.objects.set_db(self.db).update(
                        {parent_field_name: doc.pk},
                        {"$pull": {parent_field_name: doc.pk}},
                        multi=True)

        result = yield self.db[self.collection].remove(query, **kwargs)
        if result['ok'] != 1:
            # TODO how to catch this exception?
            raise OperationFailure(result, code=result['ok'])
        for doc in docs_tobe_deleted:
            yield post_remove.send(doc.__class__, document=doc)
        raise gen.Return(result)

    @gen.coroutine
    def all(self):
        params = self.get_find_extra_params()
        cursor = self.db[self.collection].find({}, **params)
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
        params = self.get_find_extra_params()
        cursor = self.db[self.collection].find(query, **params)
        return AsyncManagerCursor(self.cls, cursor, self.db,
            prefetch_related=self._prefetch_related)

    def process_query(self, query):
        for pk_name in ['id', 'pk']:
            if pk_name in query:
                _id = query.pop(pk_name)
                if isinstance(_id, SchematicsModel):
                    _id = _id.pk
                if not isinstance(_id, ObjectId) and not isinstance(_id, dict):
                    query['_id'] = ObjectId(_id)
                else:
                    query['_id'] = _id
                break
        return query

    def get_find_extra_params(self):
        params = {}
        if self.fields:
            params['fields'] = self.fields
        return params
