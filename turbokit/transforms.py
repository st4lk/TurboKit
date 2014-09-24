# -*- coding: utf-8 -*-
from schematics.transforms import (wholelist, allow_none, import_loop,
    export_loop as schematics_export_loop)
from .types import LocaleDateTimeType


def atoms(cls, instance_or_dict):
    """
    Copy of schematics.transforms.atoms (v0.9-5).
    Difference: it excludes _serializables
    """
    return ((field_name, field, instance_or_dict[field_name])
            for field_name, field in cls._fields.iteritems())


def export_loop(cls, instance_or_dict, field_converter,
                role=None, raise_error_on_role=False, print_none=False):
    """
    Copy of schematics.transforms.export_loop (v0.9-5)
    The only difference: another `atoms` function is used, that completely
    excludes serializable fields, as they must not be stored in mongodb
    """
    data = {}

    # Translate `role` into `gottago` function
    gottago = wholelist()
    if hasattr(cls, '_options') and role in cls._options.roles:
        gottago = cls._options.roles[role]
    elif role and raise_error_on_role:
        error_msg = u'%s Model has no role "%s"'
        raise ValueError(error_msg % (cls.__name__, role))
    else:
        gottago = cls._options.roles.get("default", gottago)

    for field_name, field, value in atoms(cls, instance_or_dict):
        serialized_name = field.serialized_name or field_name

        # Skipping this field was requested
        if gottago(field_name, value):
            continue

        # Value found, apply transformation and store it
        elif value is not None:
            if hasattr(field, 'export_loop'):
                shaped = field.export_loop(value, field_converter,
                                           role=role,
                                           print_none=print_none)
            else:
                shaped = field_converter(field, value)

            # Print if we want none or found a value
            if shaped is None and allow_none(cls, field):
                data[serialized_name] = shaped
            elif shaped is not None:
                data[serialized_name] = shaped
            elif print_none:
                data[serialized_name] = shaped

        # Store None if reqeusted
        elif value is None and allow_none(cls, field):
            data[serialized_name] = value
        elif print_none:
            data[serialized_name] = value

    # Return data if the list contains anything
    if len(data) > 0:
        return data
    elif print_none:
        return data


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


def to_primitive(cls, instance_or_dict, role=None, raise_error_on_role=True,
                 context=None, timezone=None):
    """
    Copy of schematics.transforms.to_promitive (v0.9-5),
    it accepts additional named argument: timezone
    """
    def field_converter(field, value):
        kwargs = dict(context=context)
        if isinstance(field, LocaleDateTimeType):
            kwargs['timezone'] = timezone
        return field.to_primitive(value, **kwargs)

    data = schematics_export_loop(cls, instance_or_dict, field_converter,
                       role=role, raise_error_on_role=raise_error_on_role)
    return data


def convert(cls, instance_or_dict, context=None, partial=True, strict=False,
            mapping=None, from_mongo=False):
    """
    Copy of schematics.transforms.convert (v0.9-5),
    it accepts additional named argument: from_mongo
    """
    def field_converter(field, value, mapping=None):
        kw_m = dict()
        kw_t = dict(mapping=mapping)
        if isinstance(field, LocaleDateTimeType):
            kw_m['from_mongo'] = from_mongo
        try:
            return field.to_native(value, **dict(kw_t.items() + kw_m.items()))
        except Exception:
            return field.to_native(value, **kw_m)
    data = import_loop(cls, instance_or_dict, field_converter, context=context,
                       partial=partial, strict=strict, mapping=mapping)
    return data
