# -*- coding: utf-8 -*-
from datetime import datetime
from turbokit.models import BaseModel, SimpleMongoModel
from turbokit.types import (ModelReferenceType, GenericModelReferenceType,
    DynamicType, LocaleDateTimeType)
from schematics import types
from schematics.types import compound
from schematics.types.serializable import serializable


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
    records = compound.ListType(ModelReferenceType, compound_field=Record)
    simplies = compound.ListType(ModelReferenceType, compound_field=SimpleModel)
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
