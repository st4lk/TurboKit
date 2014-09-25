TurboKit - async mongodb ODM for tornado
========================================

[![Build Status](https://api.travis-ci.org/ExpertSystem/TurboKit.svg?branch=master)](https://travis-ci.org/ExpertSystem/TurboKit) [![Coverage Status](https://coveralls.io/repos/ExpertSystem/TurboKit/badge.png?branch=master)](https://coveralls.io/r/ExpertSystem/TurboKit?branch=master)



Provides models to be used with [tornado](https://github.com/tornadoweb/tornado) and [mongodb](http://www.mongodb.org/) in asynchronous mode. Build on top of [schematics](https://github.com/schematics/schematics) and [motor](https://github.com/mongodb/motor).


Be aware
========

* By defalut, `schematics.types.compound.ListType` defaults to None instead of empty list. If empty list is needed, provide `default=lambda: []` as init argument.
