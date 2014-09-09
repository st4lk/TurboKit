# -*- coding: utf-8 -*-
import logging
from tornado import gen
from tornado.web import RequestHandler

l = logging.getLogger(__name__)


class BaseHandler(RequestHandler):
    pass


class HomeHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        self.render("templates/home.html")
