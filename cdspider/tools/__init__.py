#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import sys
import abc
import six
import logging
import time
import traceback
import tornado.ioloop
from cdspider import Component

@six.add_metaclass(abc.ABCMeta)
class Base(Component):
    interval = 500

    def __init__(self, context, daemon = False):
        self.ctx = context
        self.g = self.ctx.obj
        super(Base, self).__init__(self.g['logger'], logging.DEBUG if self.g['debug'] else logging.WARN)
        self.daemon = daemon
        self.ioloop = None
        self._quit = False
        self._running = False
        if daemon:
            self.ioloop = tornado.ioloop.IOLoop()

    @abc.abstractmethod
    def process(self, *args, **kwargs):
        pass

    def run_once(self, *args, **kwargs):
        self.process(*args, **kwargs)

    def run(self, *args, **kwargs):
        """
        Deamon 运行
        """
        self.info("%s starting..." % self.__class__.__name__)
        self.t = 0

        def process_loop():
            if self._quit:
                raise SystemExit
            try:
                self.process(*args, **kwargs)
                self.t += 1
                if self.t > 20:
                    self.info("%s broken" % self.__class__.__name__)
                    raise SystemExit
                time.sleep(0.1)
            except KeyboardInterrupt:
                pass
            except:
                self.error(traceback.format_exc())
                raise SystemExit

        tornado.ioloop.PeriodicCallback(process_loop, self.interval, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.info("%s exiting..." % self.__class__.__name__)

    def broken(self, message, condition):
        if not condition:
            self.show_message(message)
            if not self.daemon:
                raise SystemExit
            else:
                raise Exception(message)

    def get_arg(self, args, index, errmsg):
        assert len(args) > index, errmsg
        return args[index]

    def notice(self, message, data = None, checked = True):
        """
        notice
        """
        self.show_message(message, data)
        if checked and not self.daemon:
            x = None
            while x not in ('y', 'Y', 'n', 'N'):
                x = input('Proceed (y[Y]/n[N]):')
            if x in ('N', 'n'):
                print('Quit')
                sys.exit(0)

    def show_message(self, message, data = None):
        print('=============================================')
        print(str(message))
        if data:
            print(str(data))
        print('=============================================')
