# -*- coding: utf-8 -*-
from schematics.transforms import export_loop


def to_mongo(cls, instance_or_dict, role=None, raise_error_on_role=True,
             context=None):
    """
    Prepare data to be send to mongodb
    """
    def field_converter(field, value):
        if hasattr(field, 'to_mongo'):
            return field.to_mongo(value, context=context)
        return field.to_primitive(value, context=context)
    field_converter.to_mongo = True

    data = export_loop(cls, instance_or_dict, field_converter,
                       role=role, raise_error_on_role=raise_error_on_role)
    if hasattr(cls, '_id'):
        id_name = cls._id.serialized_name
        if id_name in data:
            data['_id'] = data.pop(id_name)
    return data
