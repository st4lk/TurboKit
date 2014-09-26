TurboKit - async mongodb ODM for tornado
========================================

[![Build Status](https://api.travis-ci.org/ExpertSystem/TurboKit.svg?branch=master)](https://travis-ci.org/ExpertSystem/TurboKit) [![Coverage Status](https://coveralls.io/repos/ExpertSystem/TurboKit/badge.png?branch=master)](https://coveralls.io/r/ExpertSystem/TurboKit?branch=master)



Provides models to be used with [tornado](https://github.com/tornadoweb/tornado) and [mongodb](http://www.mongodb.org/) in asynchronous mode. Build on top of [schematics](https://github.com/schematics/schematics) and [motor](https://github.com/mongodb/motor).


Be aware
========

* By defalut, `schematics.types.compound.ListType` defaults to None instead of empty list. If empty list is needed, provide `default=lambda: []` as init argument.

* If declaring ListType(ModelReferenceType(SomeModel, reverse_delete_rule=NULLIFY)), keep in mind, that deletion of corresponding SomeModel will nullify entire ListType field, event if it contains relations to other SomeModels. Same behaviour, as in mongoengine

* When you want to include mixin to your model, and mixin has declared model fields (instances of BaseType), this mixin must subclass schematics Model class. So fields will be handled by final model. And because of custom Options, mixins must go after BaseModel  
Example:

        from schematics.models import Model
        from schematics.types import StringType
        from turbokit.models import BaseModel

    _Good_

        class SomeMixin(Model):
            title = StringType()

        class SomeModel(BaseModel, SomeMixin):
            # ...

    _Bad (will raise an exception)_

        class SomeMixin(object):
            title = StringType()

        class SomeModel(BaseModel, SomeMixin):
            # ...

    _Also bad (will raise an exception)_

        class SomeMixin(Model):
            title = StringType()

        class SomeModel(SomeMixin, BaseModel):
            # ...

* To embed model (not a reference to model), subclass `turbokit.models.SimpleMongoModel`

* By default, `turbokit.types.LocaleDateTimeType` use utc timezone to store datetime in database. If you need another behaviour (for example, store it in server's timezone), declare it at model level in `get_database_timezone` static method. Also you have to do it in your embedded models (TODO: automatically take timezone in embedded model from parent model).
