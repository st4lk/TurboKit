# -*- coding: utf-8 -*-
from schematics.contrib.mongo import ObjectIdType as SchematicsObjectIdType
from schematics.models import Model as SchematicsModel
from bson.objectid import ObjectId


class ObjectIdType(SchematicsObjectIdType):

    def __init__(self, *args, **kwargs):
        kwargs['serialize_when_none'] = False
        super(ObjectIdType, self).__init__(*args, **kwargs)

    def to_mongo(self, value, context=None):
        if not isinstance(value, ObjectId):
            value = ObjectId(unicode(value))
        return value


class ModelReferenceType(ObjectIdType):

    def __init__(self, model_class, **kwargs):
        self.model_class = model_class
        self.fields = self.model_class.fields

        validators = kwargs.pop("validators", [])
        self.strict = kwargs.pop("strict", True)

        def validate_model(model_instance):
            # TODO
            model_instance.validate()
            return model_instance

        super(ModelReferenceType, self).__init__(
            validators=validators, **kwargs)

        # super(ModelReferenceType, self).__init__(
        #     validators=[validate_model] + validators, **kwargs)

    def to_mongo(self, value, context=None):
        # TODO: currently use SchematicsModel to avoid cycle imports
        if isinstance(value, SchematicsModel):
            value = value.pk
        return super(ModelReferenceType, self).to_mongo(value, context=context)

    def to_native(self, value, context=None):
        if isinstance(value, SchematicsModel):
            value = value.pk
        return super(ModelReferenceType, self).to_native(value, context=context)
