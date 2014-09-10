# -*- coding: utf-8 -*-
from turbokit.models import BaseModel
from schematics.types import StringType


class SimpleModel(BaseModel):
    MONGO_COLLECTION = 'simple'

    title = StringType(default='No name')
    secret = StringType()
