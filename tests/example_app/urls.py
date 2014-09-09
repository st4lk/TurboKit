# -*- coding: utf-8 -*-
from tornado.web import url
from handlers import HomeHandler


url_patterns = [
    url(r"/", HomeHandler, name="home"),
]
