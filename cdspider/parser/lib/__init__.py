#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import abc
import six
import re
from tld import get_tld
from urllib.parse import urlparse
import logging
from cdspider.libs.utils import patch_result

@six.add_metaclass(abc.ABCMeta)
class BaseParser(object):
    """
    解析基类
    """

    def __init__(self, *args, **kwargs):
        """
        init
        """
        l = len(args)
        if l > 0:
            kwargs.setdefault("ruleset", args[0])
        if l > 1:
            kwargs.setdefault("source", args[1])
        self.ruleset = kwargs.pop("ruleset", None)
        self.source = kwargs.pop("source", None)
        self.logger = kwargs.pop('logger', logging.getLogger('parser'))
        url = kwargs.pop('url', None)
        self.final_url = url
        self.domain = kwargs.pop('domain', None)
        self.subdomain = kwargs.pop('subdomain', None)
        if url:
            try:
                presult = urlparse(url)
                domain = get_tld(url)
                hostname = presult.hostname
                end = hostname.find(domain)-1
                subdomain = hostname[0:end] or ''
                self.domain = domain
                self.subdomain = subdomain
            except:
                pass
        log_level = kwargs.pop('log_level', logging.WARN)
        self._settings = kwargs or {}
        self.logger.setLevel(log_level)

    @abc.abstractmethod
    def parse(self, source = None, ruleset = None):
        """
        解析类
        """
        pass

    def patch_result(self, data, rule, callback=None):
        return patch_result(data, rule, callback)

from .JsonParser import JsonParser
from .PyqueryParser import PyqueryParser
from .XpathParser import XpathParser
from .RegularParser import RegularParser
from .goose3 import Goose
