#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import options
import motor
from urls import url_patterns
from settings import settings, MONGO_DB


class AppODM(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        mongo_host = kwargs.pop('mongo_host', MONGO_DB['host'])
        mongo_port = kwargs.pop('mongo_port', MONGO_DB['port'])
        db_connection = motor.MotorClient(host=mongo_host, port=mongo_port)
        super(AppODM, self).__init__(
            url_patterns, db_connection=db_connection, *args, **dict(settings, **kwargs))


def main():
    app = AppODM()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
