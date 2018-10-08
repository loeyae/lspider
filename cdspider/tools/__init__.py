#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import sys
import abc
import six
import logging
from cdspider import Component

@six.add_metaclass(abc.ABCMeta)
class Base(Component):

    def __init__(self, g, deamon = False):
        self.g = g
        super(Base, self).__init__(g['logger'], logging.DEBUG if g['debug'] else logging.WARN)
        self.deamon = deamon

    @abc.abstractmethod
    def process(self, *args, **kwargs):
        pass

    def run_once(self, *args, **kwargs):
        self.process(*args, **kwargs)

    def run(self, *args, **kwargs):
        while True:
            self.process(*args, **kwargs)
            time.sleep()

    def broken(self, message, condition):
        if not condition:
            self.show_message(message)
            if not self.deamon:
                sys.exit(0)
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
        if checked and not self.deamon:
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
