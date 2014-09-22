# -*- coding: utf-8 -*-
from __future__ import absolute_import
from types import MethodType
from .errors import NotRegistered

_document_registry = {}


def get_document(name):
    doc = _document_registry.get(name, None)
    if not doc:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return doc


def get_base_model():
    """ To avoid circular imports """
    from .models import BaseModel
    return BaseModel


def methodize(func, instance):
    return MethodType(func, instance, instance.__class__)
