#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-12 16:14:02
"""
import lxml
from .goose3.parsers import Parser
from . import BaseParser
from cdspider.libs.utils import table2kvlist, decode, extract_result

class XpathParser(BaseParser):
    """
    XPath Parser
    """
    def __init__(self, *args, **kwargs):
        super(XpathParser, self).__init__(*args, **kwargs)

    def parse(self, source = None, ruleset = None):
        if not source:
            source =self.source
        if not ruleset:
            ruleset = self.ruleset
        if isinstance(source, bytes):
            try:
                source = decode(source)
            except:
                pass
        try:
            doc = Parser.fromstring(source)
        except:
            return None
        if source and ruleset:
            data = {}
            for key in ruleset:
                data[key] = self._filter(doc, ruleset[key])
            return data
        return None

    def _filter(self, doc, rule, onlyOne=1):
        if isinstance(rule, dict):
            if 'filter' in rule:
                if not rule['filter']:
                    return None
                data = self.match(doc, rule['filter'], onlyOne)
                if 'item' in rule:
                    onlyOne = bool(int(rule.get('onlyOne', 0)))
                    return self._item_filter(data, rule, onlyOne)
                else:
                    rule.setdefault('type', 'text')
                    callback = rule.get('callback', None)
                    if isinstance(data, list):
                        return [self.patch_result(extract_result(self.f(item, rule), rule, None), rule, callback) for item in data]
                    return self.patch_result(extract_result(self.f(data, rule), rule, None), rule, callback)
            elif 'item' in rule:
                onlyOne = bool(int(rule.get('onlyOne', 0)))
                return self._item_filter(doc, rule, onlyOne)
            else:
                data = {}
                for k in rule:
                    data[k] = self._filter(doc, rule[k])
                return data
        elif isinstance(rule, list):
            rst = []
            for item in rule:
                data = self._filter(doc, item)
                rst.append(data)
            return rst
        else:
            return self.match(doc, rule)

    def _item_filter(self, data, rule, onlyOne):
        if isinstance(data, list):
            rst = []
            for d in data:
                rest = {}
                for item in rule['item']:
                    r = self._filter(d, rule['item'][item], onlyOne)
                    if r:
                        rest[item] = r
                rst.extend(table2kvlist(rest))
            return rst
        elif isinstance(data, dict):
            rst = []
            for idx in data:
                rest = {}
                for item in rule['item']:
                    r = self._filter(data[idx], rule['item'][item], onlyOne)
                    if r:
                        rest[item] = r
                rst.extend(table2kvlist(rest))
            return rst
        else:
            rest = {}
            for item in rule['item']:
                r = self._filter(data, rule['item'][item], onlyOne)
                if r:
                    rest[item] = r
            return table2kvlist(rest)

    def match(self, doc, rule, onlyOne = True):
        ret = Parser.xpath_re(doc, rule)
        if onlyOne:
            return ret[0]
        return ret

    def f(self, data, rule):
        if isinstance(data, lxml.html.HtmlElement):
            if rule['type'] == 'html':
                data = Parser.nodeToString(data)
            elif rule['type'] == 'attr':
                data = Parser.getAttribute(data, rule.get('target', 'value'))
            else:
                data = Parser.getText(data)
        return data
