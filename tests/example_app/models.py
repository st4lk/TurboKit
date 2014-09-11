# -*- coding: utf-8 -*-
from turbokit.models import BaseModel, SchematicsModel
from schematics import types
from schematics.types import compound


class SimpleModel(BaseModel):
    MONGO_COLLECTION = 'simple'

    title = types.StringType(default='No name')
    secret = types.StringType()


class NestedModel(SchematicsModel):
    type_string = types.StringType()
    type_int = types.IntType()

    class Options:
        serialize_when_none = False


class SchematicsFieldsModel(BaseModel):
    MONGO_COLLECTION = 'st'

    # base fields
    type_string = types.StringType()
    type_int = types.IntType()
    type_string = types.StringType()
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

    class Options:
        serialize_when_none = False
