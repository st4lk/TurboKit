# -*- coding: utf-8 -*-
from turbokit.models import BaseModel
from schematics import types


class SimpleModel(BaseModel):
    MONGO_COLLECTION = 'simple'

    title = types.StringType(default='No name')
    secret = types.StringType()
