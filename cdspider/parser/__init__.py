# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import abc
import six
import logging
from cdspider import Component
from cdspider.libs.utils import parse_domain

KNOWN_RANDOM_KEYS = [
    'r', '_', 'rm', 'f', 'callback'
]

KNOWN_TOP_LINK_PATTERN = [
    r'/thread\.php\?fid=\d+',
    r'/forum\.php\?gid=\d+',
    r'(forum-|list-funinfo-)',
]

KNOWN_DETAIL_URLS_PATTERN = [
    r'(20[012][0-9]|19[789][0-9]).?([1-9]|0[1-9]|1[012]).?([1-9]|0[1-9]|[12][0-9]|3[01]).*?\.[a-z]{3,5}',
    r'(mod=viewthread.*?tid=|read.*?tid=|viewnews-|blog-id-|do=blog&id=|thread-|post-funinfo-)',
    r'\d{2}/\d{4}/\d{2}/[\_\-\d\w]{10,}\.[a-z]{3,5}',
    r'\d{8}/[\_\-\d\w]{10,}\.[a-z]{3,5}',
    r'/\d{4}-\d{1,2}-\d{1,2}/',
    r'/[a-z0-9]{10,}/$',
    r'article/details/\d{6,}',
    r'/content\.[a-z]{3,5}',
    r'\d+\.s?html?$',
    r'/p/[a-z0-9]+(?:\?.+)?'
]


@six.add_metaclass(abc.ABCMeta)
class BaseParser(Component):
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
        log_level = kwargs.pop('log_level', logging.WARN)
        super(BaseParser, self).__init__(self.logger, log_level)
        url = kwargs.pop('url', None)
        self.final_url = url
        self.domain = kwargs.pop('domain', None)
        self.subdomain = kwargs.pop('subdomain', None)
        if url:
            subdomain, domain = parse_domain(url)
            if subdomain:
                self.subdomain = subdomain
            if domain:
                self.domain = domain
        self._settings = kwargs or {}

    @abc.abstractmethod
    def parse(self, source = None, ruleset = None):
        """
        解析类
        """
        pass


from .ListParser import ListParser
from .ItemParser import ItemParser
from .CustomParser import CustomParser
