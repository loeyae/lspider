# -*- coding: utf-8 -*-
from cdspider.libs import utils
from . import BaseParser
from .lib import Goose
from .lib import JsonParser
from .lib.goose3.text import StopWordsChinese

class CustomParser(BaseParser):
    """
    自定义解析器
    """

    def __init__(self, *args, **kwargs):
       super(CustomParser, self).__init__(*args, **kwargs)


    def parse(self, source = None, ruleset = None):
        if not source:
            source = self.source
        if not ruleset:
            ruleset = self.ruleset
        if not ruleset:
            ruleset = {}
        if "item" in ruleset:
            if 'filter' in ruleset and ruleset['filter'] and ruleset['filter'].startswith('@json:'):
                parser = JsonParser(source=source, ruleset=ruleset, logger=self.logger, domain=self.domain, subdomain=self.subdomain)
                return parser.parse()
            else:
                ruleset['onlyOne'] = 0
        elif ruleset:
            rule = list(ruleset.values())[0]
            if 'filter' in rule and rule['filter'] and rule['filter'].startswith('@json:'):
                parser = JsonParser(source=source, ruleset=ruleset, logger=self.logger, domain=self.domain, subdomain=self.subdomain)
                return parser.parse()
        onlyOne = ruleset.pop('onlyOne', 1)
        g = Goose({"target_language": "zh", 'stopwords_class': StopWordsChinese, "enable_fewwords_paragraphs": True, "logger": self.logger, "domain": self.domain, "subdomain": self.subdomain, "custom_rule": ruleset if ruleset else {}, "final_url": self.final_url})

        if isinstance(source, bytes):
            try:
                catalogue = g.parse(raw_html=utils.decode(source), encoding='UTF8')
            except:
                catalogue = g.parse(raw_html=source)
        else:
            catalogue = g.parse(raw_html=source, encoding='UTF8')
        data = catalogue.infos
        if onlyOne:
            return data[0] if (isinstance(data, list) and data) else data
        return data
