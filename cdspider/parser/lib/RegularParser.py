#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-5-29 10:44:27
"""
import re
from . import BaseParser
from cdspider.libs.utils import table2kvlist, rule2pattern, decode

class RegularParser(BaseParser):
    """
    正则解析器
    """
    def __init__(self, *args, **kwargs):
       super(RegularParser, self).__init__(*args, **kwargs)

    def parse(self, source = None, ruleset = None):
        if not source:
            source = self.source
        if not ruleset:
            ruleset = self.ruleset
        if isinstance(source, bytes):
            try:
                source = decode(source)
            except:
                source = str(source)
        if source and ruleset:
            data = {}
            for key in ruleset:
                data[key] = self._filter(source, ruleset[key])
            return data
        return None

    def _filter(self, doc, rule, onlyOne=1):
        if isinstance(rule, dict):
            if 'filter' in rule:
                data = self.match(doc, rule['filter'], onlyOne)
                if 'item' in rule:
                    onlyOne = bool(int(rule.get('onlyOne', 0)))
                    return self._item_filter(data, rule, onlyOne)
                else:
                    callback = rule.get('callback', None)
                    return self.patch_result(data, rule, callback)
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
        if rule.startswith('@reg:'):
            rule = rule[5:]
        rule, key = rule2pattern(rule)
        if not rule or not key:
            return None
        matched = None
        if onlyOne:
            r = re.search(rule, doc, re.S|re.I)
            if r:
                matched = r.group(key)
                if matched:
                    matched = matched.strip()
        else:
            matched = re.findall(rule, doc, re.S|re.I)
        return matched
