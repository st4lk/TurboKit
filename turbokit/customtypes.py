# -*- coding: utf-8 -*-
from schematics.contrib.mongo import ObjectIdType as SchematicsObjectIdType
from bson.objectid import ObjectId


class ObjectIdType(SchematicsObjectIdType):
    def to_mongo(self, value, context=None):
        if not isinstance(value, ObjectId):
            value = ObjectId(value)
        return value


class ModelReferenceType(ObjectIdType):

    def __init__(self, model_class, **kwargs):
        self.model_class = model_class
        self.fields = self.model_class.fields

        validators = kwargs.pop("validators", [])
        self.strict = kwargs.pop("strict", True)

        def validate_model(model_instance):
            model_instance.validate()
            return model_instance

        super(ModelReferenceType, self).__init__(
            validators=validators, **kwargs)

        # super(ModelReferenceType, self).__init__(
        #     validators=[validate_model] + validators, **kwargs)

    def to_primitive(self, value, context=None, expand_related=False):
        return str(value)
