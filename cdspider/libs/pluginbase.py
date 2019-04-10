#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019/4/10 16:54
"""

import six
import abc

@six.add_metaclass(abc.ABCMeta)
class ExecutBase(object):
    """
    Base class for example plugin used in the tutorial.
    """

    def __init__(self, handler):
        self.handler = handler

    @abc.abstractmethod
    def handle(self, *args, **kwargs):
        """Format the data and return unicode text.

        :param data: A dictionary with string keys and simple types as
                     values.
        :type data: dict(str:?)
        :returns: Iterable producing the formatted text.
        """