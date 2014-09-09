# -*- coding: utf-8 -*-
from turbokit.models import BaseModel
from schematics.types import StringType
# from utils import get_db_connection


# class BaseExampleModel(BaseModel):
#     connection = get_db_connection()


class SimpleModel(BaseModel):
    MONGO_COLLECTION = 'simple'

    title = StringType(default='No name')
    secret = StringType()
