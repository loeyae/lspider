#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import sys
import abc
import six
import logging

@six.add_metaclass(abc.ABCMeta)
class Base(object):

    def __init__(self, g, no_loop = False):
        self.g = g
        self.logger = g['logger']
        self.logger.setLevel(logging.DEBUG if g['debug'] else logging.WARN)
        self.no_loop = no_loop

    @abc.abstractmethod
    def process(self, *args, **kwargs):
        pass

    def notice(self, message, data = None, checked = True):
        """
        notice
        """
        print('=============================================')
        print(str(message))
        if data:
            print(str(data))
        print('=============================================')
        if checked:
            x = None
            while x not in ('y', 'Y', 'n', 'N'):
                x = input('Proceed (y[Y]/n[N]):')
            if x in ('N', 'n'):
                print('Quit')
                sys.exit(0)
