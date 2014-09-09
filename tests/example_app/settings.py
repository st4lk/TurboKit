# -*- coding: utf-8 -*-
from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
define("config", default=None, help="tornado config file")
define("debug", default=True, help="debug mode")

settings = {
    'debug': options.debug,
}

# Mongo settings
MONGO_DB = {
    'host': '127.0.0.1',
    'port': 27017,
    'db_name': "odm_example",
}
