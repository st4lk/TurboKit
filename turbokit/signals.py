# -*- coding: utf-8 -*-
from blinker import Namespace as BlinkerNamespace, Signal as BlinkerSignal
from blinker._utilities import lazy_property
from tornado import gen


class AsyncSignal(BlinkerSignal):
    """
    Copy of blinker.Signal, but call receivers in async mode.
    Asserted that all receivers are wrapped with tornado.gen.coroutine
    """
    @lazy_property
    def receiver_connected(self):
        raise NotImplementedError()

    @lazy_property
    def receiver_disconnected(self):
        raise NotImplementedError()

    @gen.coroutine
    def send(self, *sender, **kwargs):
        """Emit this signal on behalf of *sender*, passing on \*\*kwardf.

        Returns a list of 2-tuples, pairing receivers with their return
        value. The ordering of receiver notification is undefined.

        :param \*sender: Any object or ``None``.  If omitted, synonymous
          with ``None``.  Only accepts one positional argument.

        :param \*\*kwargs: Data to be sent to receivers.

        """
        # Using '*sender' rather than 'sender=None' allows 'sender' to be
        # used as a keyword argument- i.e. it's an invisible name in the
        # function signature.
        if len(sender) == 0:
            sender = None
        elif len(sender) > 1:
            raise TypeError('send() accepts only one positional argument, '
                            '%s given' % len(sender))
        else:
            sender = sender[0]
        if not self.receivers:
            results = []
        else:
            results = []
            for receiver in self.receivers_for(sender):
                result = yield receiver(sender, **kwargs)
                results.append((receiver, result))
        raise gen.Return(results)


class AsyncNamedSignal(AsyncSignal):
    """Copy of blinker.NamedSignal, but use AsyncSignal"""

    def __init__(self, name, doc=None):
        AsyncSignal.__init__(self, doc)

        #: The name of this signal.
        self.name = name

    def __repr__(self):
        base = AsyncSignal.__repr__(self)
        return "%s; %r>" % (base[:-1], self.name)


class AsyncNamespace(BlinkerNamespace):
    """Copy of blinker.Namespace, but use AsyncNamedSignal"""

    def signal(self, name, doc=None):
        """Return the :class:`AsyncNamedSignal` *name*, creating it if required.

        Repeated calls to this function will return the same signal object.

        """
        try:
            return self[name]
        except KeyError:
            return self.setdefault(name, AsyncNamedSignal(name, doc))


_signals = AsyncNamespace()

pre_save = _signals.signal('pre_save')
post_save = _signals.signal('post_save')
pre_remove = _signals.signal('pre_remove')
post_remove = _signals.signal('post_remove')
