#!/usr/bin/python
# -*- coding: UTF-8 -*-
# date: 2018/1/23
# author: Mr wang
from cdspider.libs import Goose
from cdspider.libs import utils
from cdspider.libs.goose3.text import StopWordsChinese
from cdspider.parser import BaseParser
from .JsonParser import JsonParser
from .XmlParser import XmlParser

class ListParser(BaseParser):
    """
    列表页自动解析器
    """

    def __init__(self, *args, **kwargs):
       super(ListParser, self).__init__(*args, **kwargs)


    def parse(self, source = None, ruleset = None):
        if not source:
            source = self.source
        if not ruleset:
            ruleset = self.ruleset
        if not ruleset:
            ruleset = {}
        if 'filter' in ruleset and ruleset['filter'] and ruleset['filter'].startswith('@json:'):
            parser = JsonParser(source=source, ruleset=ruleset, logger=self.logger, domain=self.domain, subdomain=self.subdomain)
            return parser.parse()
        if 'filter' in ruleset and ruleset['filter'] and ruleset['filter'].startswith('@xml:'):
            parser = XmlParser(source=source, ruleset=ruleset, logger=self.logger, domain=self.domain, subdomain=self.subdomain)
            return parser.parse()
        g = Goose({"target_language": "zh", 'stopwords_class': StopWordsChinese, "enable_fewwords_paragraphs": True, "logger": self.logger, "domain": self.domain, "subdomain": self.subdomain, "custom_rule": ruleset if ruleset else {}})

        if isinstance(source, bytes):
            try:
                catalogue = g.fetch(raw_html=utils.decode(source), encoding='UTF8')
            except:
                catalogue = g.fetch(raw_html=source)
        else:
            catalogue = g.fetch(raw_html=source, encoding='UTF8')
        data = catalogue.infos
        return data
