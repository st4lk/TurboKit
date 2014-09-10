# -*- coding: utf-8 -*-
from turbokit.models import BaseModel
from schematics import types


class SimpleModel(BaseModel):
    MONGO_COLLECTION = 'simple'

    title = types.StringType(default='No name')
    secret = types.StringType()


class SchematicsBaseFieldsModel(BaseModel):
    MONGO_COLLECTION = 'basetypes'

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
