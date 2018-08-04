#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: __init__.py 587 2018-01-26 03:18:14Z zhangyi $

import abc
import six
import logging

@six.add_metaclass(abc.ABCMeta)
class BaseSender(object):

    sender = None
    receiver = None

    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', logging.getLogger('mailer'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self.sender = kwargs.pop('sender')
        self.receiver = kwargs.pop('receiver')

    @abc.abstractmethod
    def send(self, subject, message, type = "plain", to_list = None):
        """
        发送邮件
        """
        pass

from .SmtpSender import SmtpSender
