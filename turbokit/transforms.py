# -*- coding: utf-8 -*-
from schematics.transforms import export_loop
from .customtypes import ModelReferenceType


def to_primitive(cls, instance_or_dict, role=None, raise_error_on_role=True,
                 context=None, expand_related=False):
    """
    Accepts additional parameter: expand_related. If True,
    referenced model objects will be expanded to primitive and included in
    output, instead of just id.
    For base documentation of this method, look schematics.transforms.to_primitive
    """
    # field_converter = lambda field, value: field.to_primitive(value,
                                                              # context=context)
    def field_converter(field, value):
        if expand_related and isinstance(field, ModelReferenceType):
            return field.to_primitive(value, context=context, expand_related=expand_related)
        return field.to_primitive(value, context=context)

    data = export_loop(cls, instance_or_dict, field_converter,
                       role=role, raise_error_on_role=raise_error_on_role)
    return data
