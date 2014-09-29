# -*- coding: utf-8 -*-
from datetime import datetime
from turbokit.models import BaseModel, SimpleMongoModel
from turbokit.types import (ModelReferenceType, GenericModelReferenceType,
    DynamicType, LocaleDateTimeType, DO_NOTHING, NULLIFY, CASCADE, DENY, PULL)
from schematics import types
from schematics.types import compound
from schematics.types.serializable import serializable
from schematics.models import Model


class SimpleModel(BaseModel):
    title = types.StringType(default='No name')
    secret = types.StringType()

    def __unicode__(self):
        return self.title


class User(BaseModel):
    name = types.StringType()
    age = types.IntType()

    def __unicode__(self):
        return self.name


class Event(BaseModel):
    title = types.StringType()
    user = ModelReferenceType(User)


class Record(BaseModel):
    title = types.StringType()
    event = ModelReferenceType(Event)
    simple = ModelReferenceType(SimpleModel)


class RecordSeries(BaseModel):
    title = types.StringType()
    records = compound.ListType(ModelReferenceType(Record))
    simplies = compound.ListType(ModelReferenceType(SimpleModel))
    main_event = ModelReferenceType(Event)


class NestedModel(SimpleMongoModel):
    type_string = types.StringType()
    type_int = types.IntType()

    class Options:
        serialize_when_none = False


class Transaction(BaseModel):
    title = types.StringType()
    item = GenericModelReferenceType()


class Page(BaseModel):
    title = types.StringType()
    content = DynamicType()


class Brand(BaseModel):
    title = types.StringType()
    menu = compound.ListType(DynamicType)


class Plan(BaseModel):
    title = types.StringType(default='default')
    ends_at = types.DateTimeType(default=datetime.now)

    @serializable
    def is_expired(self):
        return self.ends_at < datetime.now()


class Topic(BaseModel):
    title = types.StringType(default='best')
    ancestor = ModelReferenceType('self')


class Action(BaseModel):
    start_at = LocaleDateTimeType()


class ActionDefaultDate(BaseModel):
    start_at = LocaleDateTimeType(default=datetime.now)


class ActionMixin(Model):
    start_at = LocaleDateTimeType()


class ActionWithMixin(BaseModel, ActionMixin):
    title = types.StringType(default="morning")


class ActionBase(BaseModel):
    start_at = LocaleDateTimeType()


class ActionSubclassed(ActionBase):
    title = types.StringType(default="pool")


class ChildA(BaseModel):
    pass


class ParentA(BaseModel):
    child = ModelReferenceType(ChildA, reverse_delete_rule=DO_NOTHING)


class ParentB(BaseModel):
    child = ModelReferenceType(ChildA, reverse_delete_rule=NULLIFY)


class ParentC(BaseModel):
    child = ModelReferenceType(ChildA, reverse_delete_rule=CASCADE)


class ParentD(BaseModel):
    child = ModelReferenceType(ChildA, reverse_delete_rule=DENY)


class ParentE(BaseModel):
    childs = compound.ListType(ModelReferenceType(ChildA, reverse_delete_rule=DO_NOTHING))


class ParentF(BaseModel):
    childs = compound.ListType(ModelReferenceType(ChildA, reverse_delete_rule=NULLIFY))


class ParentG(BaseModel):
    childs = compound.ListType(ModelReferenceType(ChildA, reverse_delete_rule=CASCADE))


class ParentH(BaseModel):
    childs = compound.ListType(ModelReferenceType(ChildA, reverse_delete_rule=DENY))


class ParentI(BaseModel):
    childs = compound.ListType(ModelReferenceType(ChildA, reverse_delete_rule=PULL))


class ChildB(BaseModel):
    pass


class ChildC(BaseModel):
    pass


class ParentMixin(Model):
    """
    Need to subclass schematics.Model, so declared fields will be collected
    """
    friends = compound.ListType(ModelReferenceType(ChildB, reverse_delete_rule=PULL))


class ParentK(BaseModel, ParentMixin):
    guru = ModelReferenceType(ChildC, reverse_delete_rule=NULLIFY)


class ParentBase(BaseModel):
    friends = compound.ListType(ModelReferenceType(ChildB, reverse_delete_rule=PULL))


class ParentSublcassed(ParentBase):
    guru = ModelReferenceType(ChildC, reverse_delete_rule=NULLIFY)


class SchematicsFieldsModel(BaseModel):
    # base fields
    type_string = types.StringType()
    type_int = types.IntType()
    type_uuid = types.UUIDType()
    type_IPv4 = types.IPv4Type()
    type_url = types.URLType()
    type_email = types.EmailType()
    type_number = types.NumberType(int, "integer")
    type_int = types.IntType()
    type_long = types.LongType()
    type_float = types.FloatType()
    type_decimal = types.DecimalType()
    type_md5 = types.MD5Type()
    type_sha1 = types.SHA1Type()
    type_boolean = types.BooleanType()
    type_date = types.DateType()
    type_datetime = types.DateTimeType()
    type_geopoint = types.GeoPointType()
    # type_multilingualstring = types.MultilingualStringType(default_locale='localized_value')

    # compound fields
    type_list = compound.ListType(types.StringType)
    type_dict = compound.DictType(types.IntType)  # dict values can be only integers
    type_list_of_dict = compound.ListType(compound.DictType, compound_field=types.StringType)
    type_dict_of_list = compound.DictType(compound.ListType, compound_field=types.IntType)
    type_model = compound.ModelType(NestedModel)
    type_list_model = compound.ListType(compound.ModelType(NestedModel))

    # reference fields
    type_ref_simplemodel = ModelReferenceType(SimpleModel)
    type_ref_usermodel = ModelReferenceType(User)

    class Options:
        # namespace = 'st'
        collection = 'st'
        serialize_when_none = False
