# -*- coding: utf-8 -*-
from __future__ import absolute_import
from types import MethodType


def methodize(func, instance):
    return MethodType(func, instance, instance.__class__)
