# -*- coding: utf-8 -*-
import tzlocal
from datetime import datetime
from dateutil import parser
from schematics.contrib.mongo import ObjectIdType as SchematicsObjectIdType
from schematics.exceptions import ValidationError, ConversionError
from schematics.transforms import export_loop
from schematics.types import TypeMeta, BaseType, DateTimeType
from bson.objectid import ObjectId
from .utils import get_base_model, get_simple_model, get_model


# Delete rules
DO_NOTHING = 0
NULLIFY = 1
CASCADE = 2
DENY = 3
PULL = 4


class ObjectIdType(SchematicsObjectIdType):

    def __init__(self, *args, **kwargs):
        kwargs['serialize_when_none'] = False
        super(ObjectIdType, self).__init__(*args, **kwargs)

    def to_mongo(self, value, context=None):
        if not isinstance(value, ObjectId):
            value = ObjectId(unicode(value))
        return value


class ObjectIdWithLen(ObjectId):
    def __len__(self):
        """
        According to implementation of DictType and ListType, if field
        has `export_loop` method, then it must be able to process `len()`.
        Without it, such construction will not work properly:
        compound.ListType(ModelReferenceType, compound_field=SomeModel)

        """
        return 1


class ModelReferenceMeta(TypeMeta):
    def __new__(cls, name, bases, attrs):
        """
        Remove `ObjectIdType.validate_id` from validators.
        Without it, this method will be called, even if we redefine it in
        child class.
        """
        for b in bases:
            if hasattr(b, '_validators'):
                b._validators = filter(lambda x: 'validate_id' != x.func_name,
                    b._validators)
        return super(ModelReferenceMeta, cls).__new__(cls, name, bases, attrs)


class ModelReferenceType(ObjectIdType):
    __metaclass__ = ModelReferenceMeta

    def __init__(self, field, reverse_delete_rule=DO_NOTHING, **kwargs):
        """
        Keep name `field`, as schematics.types.compound.MultiType
        use this name in init_compound_field
        """
        self._model_class = field
        self.reverse_delete_rule = reverse_delete_rule

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

    @property
    def model_class(self):
        """
        To handle 'self' reference, we need to know the owner_model class
        But it is not avaliable in __init__ of this field, this
        property will be attached after __init__.
        Look schematics.models.ModelMeta for details
        """
        if isinstance(self._model_class, basestring):
            if self._model_class == 'self':
                self._model_class = self.owner_model
            else:
                raise NotImplementedError("String class reference TBD")
        return self._model_class

    @property
    def fields(self):
        return self.model_class.fields

    def validate_id(self, value):
        if isinstance(value, get_base_model()):
            # TODO: validate full model in separate method, validate_model
            value = value.pk
        return super(ModelReferenceType, self).validate_id(value)

    def to_mongo(self, value, context=None):
        if isinstance(value, get_base_model()):
            value = value.pk
        return ObjectIdWithLen(value)

    def to_native(self, value, context=None):
        if isinstance(value, get_base_model()):
            return value
        return super(ModelReferenceType, self).to_native(value, context=context)

    def export_loop(self, model_instance, field_converter,
                    role=None, print_none=False):
        if not isinstance(model_instance, self.model_class) or\
                getattr(field_converter, 'to_mongo', False):
            return field_converter(self, model_instance)
        else:
            model_class = model_instance.__class__
            shaped = export_loop(model_class, model_instance,
                                 field_converter,
                                 role=role, print_none=print_none)

            if shaped and len(shaped) == 0 and self.allow_none():
                return shaped
            elif shaped:
                return shaped
            elif print_none:
                return shaped


class DynamicType(BaseType):
    # TODO: process BaseModel

    def validate_dynamic(self, value):
        if isinstance(value, get_base_model()):
            raise ValidationError("Saving instance of BaseModel currently not implemented")
        if hasattr(value, 'validate'):
            # TODO: pass partial, strict. Look for schematics.modelsModel.validate
            value.validate()

    def to_mongo(self, value, context=None):
        if hasattr(value, 'to_mongo'):
            if isinstance(value, get_simple_model()):
                data = value.to_mongo()
                data['_cls'] = value._cls_key
                return data
        return value

    def to_native(self, value, context=None):
        if isinstance(value, dict):
            if '_cls' in value:
                _cls = value.pop('_cls')
                if '_id' in value:  # TODO: process BaseModel
                    pass
                else:
                    model_class = get_model(_cls)
                    value = model_class(value)
        return value


class GenericModelReferenceType(ObjectIdType):
    #TODO: implement functionality, covered by
    # TestSerializationGenericReferenceList and TestSerializationGenericModelReference

    __metaclass__ = ModelReferenceMeta

    MESSAGES = {
        'bad_value': (u"Value of {self.__class__.__name__} must be instance of"
            " {base_model.__name__} or dict with keys ('_cls', '_id')"),
        'no_id': u"Model is not saved yet. Save it first",
        'no_cls': u"Class of model can't be determind",
    }

    def __init__(self, *args, **kwargs):
        self._cls = None
        super(GenericModelReferenceType, self).__init__(*args, **kwargs)

    def validate_class(self, value):
        """
        Currently only already saved model objects can be saved as generic
        relation.
        """
        base_model = get_base_model()
        _id = None
        if isinstance(value, base_model):
            if not value.pk:
                raise ValidationError(self.messages['no_id'])
            _id = value.pk
        elif isinstance(value, ObjectId):
            if not self._cls:
                raise ValidationError(self.messages['no_cls'])
            _id = value
        elif isinstance(value, dict):
            if set(value.keys()) != set(('_cls', '_id')):
                raise ValidationError(self.messages['bad_value'].format(
                    self=self, base_model=base_model))
            if not value['_id']:
                raise ValidationError(self.messages['no_id'])
            _id = value['_id']
        else:
            raise ValidationError(self.messages['bad_value'].format(
                self=self, base_model=base_model))
        return super(GenericModelReferenceType, self).validate_id(_id)

    def to_mongo(self, value, context=None):
        if isinstance(value, get_base_model()):
            value = {"_id": value.pk, "_cls": value._cls_key}
        elif isinstance(value, ObjectId):
            value = {"_id": value, "_cls": self._cls}
        return value

    def to_native(self, value, context=None):
        base_model = get_base_model()
        if isinstance(value, base_model):
            return value
        if isinstance(value, dict):
            if set(value.keys()) != set(('_cls', '_id')):
                raise ConversionError(self.messages['bad_value'].format(
                    self=self, base_model=base_model))
            self._cls = value['_cls']
            return value['_id']
        return super(GenericModelReferenceType, self).to_native(value, context=context)


class LocaleDateTimeType(DateTimeType):
    """
    Type to work with localized datetimes.
    If naive datetime is given, it is assumed, that such value has timezone
    of current machine.
    It automatically converts given datetime to database timezone.
    Database timezone is specified in model_class.get_database_timezone()
    To serialize datetime back in any timezone, specify it as argument
    in `to_primitive` method.
    """

    def __init__(self, formats=None, serialized_format=None,
                serialize_as_isoformat=True, **kwargs):
        if not serialized_format and serialize_as_isoformat:
            serialized_format = self.render_isoformat
        super(LocaleDateTimeType, self).__init__(formats=formats,
            serialized_format=serialized_format, **kwargs)

    @classmethod
    def render_isoformat(cls, value, timezone=None):
        timezone = timezone or tzlocal.get_localzone()
        return value.astimezone(timezone).isoformat()

    def to_primitive(self, value, context=None, timezone=None):
        if callable(self.serialized_format):
            return self.serialized_format(value, timezone=timezone)
        timezone = timezone or tzlocal.get_localzone()
        return value.astimezone(timezone).strftime(self.serialized_format)

    def to_mongo(self, value, context=None):
        return self.to_native(value, context=context)

    def to_native(self, value, context=None, from_mongo=False):
        if isinstance(value, datetime):
            dt_value = value
        else:
            dt_value = parser.parse(value)
        if from_mongo:
            # datetime is naive, but assume, that it has database_timezone
            dt_value = self.owner_model.get_database_timezone().localize(dt_value)
        else:
            dt_value = self.convert_to_database_tz(dt_value)
        return dt_value

    def convert_to_database_tz(self, dt_value):
        if not dt_value.tzinfo:
            # dt_value is naive, assume it has current server timezone
            local_tz = tzlocal.get_localzone()
            dt_value = local_tz.localize(dt_value)
        if dt_value.tzinfo != self.owner_model.get_database_timezone():
            dt_value = dt_value.astimezone(self.owner_model.get_database_timezone())
        return dt_value
