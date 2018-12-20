#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-24 21:39:45
"""
import six
import re
import json
import copy
import traceback
from . import BaseParser
from .PyqueryParser import PyqueryParser
from .RegularParser import RegularParser
from .XpathParser import XpathParser
from cdspider.exceptions import *
from cdspider.libs import utils

class JsonParser(BaseParser):
    """
    JSON Parser
    """
    def __init__(self, *args, **kwargs):
       super(JsonParser, self).__init__(*args, **kwargs)

    def parse(self, source = None, ruleset = None):
        """
        根据规则解析内容
        """
        if not source:
            source =self.source
        if not ruleset:
            ruleset = self.ruleset
        if isinstance(source, bytes):
            try:
                source = utils.decode(source)
            except:
                source = str(source)
#        self.info("Json source: %s" % re.sub(r"(\r|\n|\s{2,})", "", str(source)))
#        self.info("Json ruleset: %s" % str(ruleset))
        if source and ruleset:
            source = source.strip()
            ll = source[0:1]
            rl = source[-1:]
            if ll == '{' and rl == '}':
                data = self._jsonparse(source, ruleset)
            elif ll == '[' and rl == ']':
                data = self._jsonparse(source, ruleset)
            else:
                haslp = source.find('(')
                hasrp = source.rfind(')')
                if haslp == -1 or hasrp == -1:
                    raise CDSpiderParserJsonLoadFaild('Invalid json data: %s' % (re.sub(r'\s{2,}', ' ', str(source).replace("\r", "").replace("\n",""))), rule = ruleset)
                data = self._jsonparse(source[haslp+1:hasrp], ruleset)
            return data
        return source

    def _jsonparse(self, text, ruleset):
        rule = ruleset.copy()
        try:
            if isinstance(text, six.string_types):
                data = json.loads(text)
            else:
                data = text
        except Exception:
            data = None

        if not data:
            raise CDSpiderParserJsonLoadFaild('Invalid json data: %s' % (re.sub(r'\s{2,}', ' ', str(text).replace("\r", "").replace("\n",""))), rule = ruleset)
        if rule:
            return self._filter(data, rule)

    def _filter(self, data, rule):
        if isinstance(rule, dict):
            if 'filter' in rule:
                if not rule['filter']:
                    return None
                if rule['filter'].startswith("@json:"):
                    rule['filter'] = rule['filter'][6:]
                data = self._json_parse(data, rule['filter'])
                if data is None:
                    return None
                if 'item' in rule:
                    onlyOne = bool(int(rule.get('onlyOne', 0)))
                    return self._item_filter(data, rule, onlyOne)
                else:
                    callback = rule.get('callback', None)
                    return self.patch_result(data, rule, callback)
            elif 'item' in rule:
                onlyOne = bool(int(rule.get('onlyOne', 0)))
                return self._item_filter(data, rule, onlyOne, noLeaf = True)
            else:
                rst = {}
                for key, val in rule.items():
                    rst[key] = self._filter(data, val)
                return rst
        elif isinstance(rule, list):
            rst = []
            for item in rule:
                rest = self._filter(copy.deepcopy(data), item)
                if rest:
                    rst.append(rest)
            return rst
        else:
            return self._filter(data, {"filter": rule})

    def _item_filter(self, data, rule, onlyOne, noLeaf = False):
        if isinstance(data, list):
            rst = []
            for d in data:
                rest = self._item_filter(d, rule, onlyOne, noLeaf = True)
                if rest:
                    rst.extend(rest)
                    if onlyOne:
                        return rst
            return rst
        elif not noLeaf and isinstance(data, dict):
            rst = []
            for idx in data:
                rest = self._item_filter(data[idx], rule, onlyOne, noLeaf = True)
                if rest:
                    rst.extend(rest)
                    if onlyOne:
                        return rst
            return rst
        else:
            ruleset = rule['item']['url'] if 'url' in rule['item'] else list(rule['item'].values())[0]
            if 'filter' in ruleset and ruleset['filter'] and ruleset['filter'].startswith('@css:'):
                for k, v in rule['item'].items():
                    if v['filter']:
                        v['filter'] = v['filter'][5:]
                parser = PyqueryParser(ruleset={"json": {"onlyOne": onlyOne, 'item': rule['item']}}, source=copy.deepcopy(data))
                parsed = parser.parse()
                return parsed.get('json', []) if parsed else None
            elif 'filter' in ruleset and ruleset['filter'].startswith('@xpath:'):
                for k, v in rule['item'].items():
                    if v['filter']:
                        v['filter'] = v['filter'][7:]
                parser = XpathParser(ruleset={"json": {"onlyOne": onlyOne, 'item': rule['item']}}, source=copy.deepcopy(data))
                parsed = parser.parse()
                return parsed.get('json', []) if parsed else None
            elif 'filter' in ruleset and ruleset['filter'] and ruleset['filter'].startswith('@reg:'):
                for k, v in rule['item'].items():
                    v['filter'] = v['filter'][5:]
                parser = RegularParser(ruleset={"json": {"onlyOne": onlyOne, 'item': rule['item']}}, source=copy.deepcopy(data))
                parsed = parser.parse()
                return parsed.get('json', []) if parsed else None
            else:
                rest = {}
                for item in rule['item']:
                    rest[item] = self._filter(copy.deepcopy(data), rule['item'][item])
                return [rest]

    def _json_parse(self, data, kstring):
        kstring = "[\"%s\"]" % kstring.replace(".", "\"][\"")
        result = None
        code = "data%s" % re.sub(r"\[\"(\d+)\"\]", r"[\1]", kstring)
        try:
            result = eval(code, None, locals())
        except:
            self.logger.error(traceback.format_exc())
            pass
        return result
