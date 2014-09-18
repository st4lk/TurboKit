# -*- coding: utf-8 -*-
import logging
from tornado import gen
from schematics.types import compound
from .customtypes import ModelReferenceType

l = logging.getLogger(__name__)


class PrefetchRelatedMixin(object):
    def __init__(self, *args, **kwargs):
        self._prefetch_related = set(kwargs.get('prefetch_related', []))

    def prefetch_related(self, *args):
        self._prefetch_related |= set(args)
        return self

    @gen.coroutine
    def fetch_related_objects(self, objects_list, related_fields=None):
        prefetch_data = []
        if objects_list:
            parent_model = objects_list[0]
            if related_fields is None:
                related_fields = self._prefetch_related
            for pr_full in related_fields:
                if "." in pr_full:
                    pr, pr_left = pr_full.split(".", 1)
                else:
                    pr, pr_left = pr_full, None
                field = parent_model._fields.get(pr, None)
                if field:
                    if isinstance(field, ModelReferenceType) \
                            or (isinstance(field, compound.ListType)
                                and isinstance(field.field, ModelReferenceType)):
                        prefetch_data.append((field, pr, pr_left,
                            map(lambda m: getattr(m, pr), objects_list)))
                    else:
                        l.warning("can't prefetch field {0}, class: {1}"
                            .format(field.__class__.__name__, parent_model.__class__.__name__))
                else:
                    l.warning("Unknown field '{0}' in '{1}.prefetch_related'"
                        .format(pr, parent_model.__class__.__name__))
        for pr_field, pr_field_name, pr_child_field_names, ids in prefetch_data:
            is_list = isinstance(pr_field, compound.ListType)
            if is_list:
                #TODO: refactor to be more effective or maybe list(set()) is odd
                ids_expanded = list(set([item for sublist in ids for item in sublist]))
            else:
                ids_expanded = ids
            cursor = self.db[pr_field.model_class.MONGO_COLLECTION]\
                .find({"_id": {"$in": ids_expanded}})
            pr_data_list = yield cursor.to_list(None)
            pr_model_list = map(pr_field.model_class, pr_data_list)
            if pr_child_field_names:
                # fetch child related fields recursively
                pr_model_list = yield self.fetch_related_objects(pr_model_list,
                    related_fields=set([pr_child_field_names]))
            pr_model_dict = dict(map(lambda x: (x.pk, x), pr_model_list))
            for m in objects_list:
                f_value_was = getattr(m, pr_field_name)
                if is_list:
                    f_values_is = [pr_model_dict[pk] for pk in f_value_was]
                else:
                    f_values_is = pr_model_dict[f_value_was]
                setattr(m, pr_field_name, f_values_is)
        raise gen.Return(objects_list)


class AsyncManagerCursor(PrefetchRelatedMixin):

    def __init__(self, cls, cursor, db=None, **kwargs):
        super(AsyncManagerCursor, self).__init__(cls, cursor, db=None, **kwargs)
        self.cursor = cursor
        self.cls = cls
        self.db = db

    @property
    def fetch_next(self):
        return self.cursor.fetch_next

    def next_object(self):
        result = self.cursor.next_object()
        return self.cls(result)

    def sort(self, *args, **kwargs):
        self.cursor = self.cursor.sort(*args, **kwargs)
        return self

    def skip(self, *args, **kwargs):
        self.cursor = self.cursor.skip(*args, **kwargs)
        return self

    def limit(self, *args, **kwargs):
        self.cursor = self.cursor.limit(*args, **kwargs)
        return self

    @gen.coroutine
    def __getitem__(self, index, *args, **kwargs):
        result = None
        if isinstance(index, slice):
            self.cursor = self.cursor[index]
            result = yield self.all()
        elif isinstance(index, (int, long)):
            self.cursor = self.cursor[index:index+1]
            result = yield self.all()
            result = result[0]
        else:
            raise TypeError(u"index {0} cannot be applied to Cursor "
                            u"instances".format(index))
        raise gen.Return(result)

    @gen.coroutine
    def all(self):
        response = yield self.cursor.to_list(None)
        results = [self.cls(d) for d in response]
        results_with_related = yield self.fetch_related_objects(results)
        raise gen.Return(results_with_related)
