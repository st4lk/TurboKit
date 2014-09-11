# -*- coding: utf-8 -*-
from types import MethodType


def methodize(func, instance):
    return MethodType(func, instance, instance.__class__)
