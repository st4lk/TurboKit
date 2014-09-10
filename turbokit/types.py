# -*- coding: utf-8 -*-
from schematics.types import NumberType
from bson.objectid import ObjectId


class ObjectIdType(NumberType):
    def __init__(self, number_class=ObjectId, number_type="ObjectId", **kwargs):
        super(ObjectIdType, self).__init__(number_class, number_type, **kwargs)

    def to_primitive(self, value, context=None):
        return str(value)
