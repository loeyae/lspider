#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:58:01
"""
import six
from pyquery import PyQuery
from . import BaseParser
from cdspider.libs.utils import pcre2re
from cdspider.libs.utils import callback_result
from cdspider.libs.utils import decode

class PyqueryParser(BaseParser):
    """
    基于pyquery的数据解析
    """

    def __init__(self, *args, **kwargs):
        super(PyqueryParser, self).__init__(*args, **kwargs)

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
                source = decode(source)
            except:
                pass
#        self.logger.info("Pyquery source: %s" % re.sub(r"(\r|\n|\s{2,})", "", str(content)))
#        self.logger.info("Pyquery ruleset: %s" % str(ruleset))
        if source and ruleset:
            pq = PyQuery(source)
            data = {}
            for k in ruleset:
                if not ruleset[k]:
                    data[k] = source
                    continue
                if 'type' in ruleset[k] or 'filter' in ruleset[k]:
                    self.logger.info("Pyquery rule: %s" % str(ruleset[k]))
                    data[k] = self._filter(pq, ruleset[k])
                    self.logger.info("Pyquery data: %s" % str(data[k]))
                else:
                    rule = ruleset[k]
                    if isinstance(rule, list):
                        for r in rule:
                            self.logger.info("rule: %s" % str(r))
                            item = self._filter(pq, r)
                            self.logger.info("data: %s" % str(item))
                            if item:
                                data[k] = item
                                break
                    else:
                        rest = {}
                        for idx in rule:
                            self.logger.info("Pyquery rule: %s=>%s" % (idx, str(rule[idx])))
                            rest[idx] = self._filter(pq, rule[idx])
                            self.logger.info("Pyquery data: %s=>%s" % (idx, str(rest[idx])))
                        data[k] = rest
            return data
        return source

    def _filter(self, pq, rule = None):
        """
        根据配置筛选页面内容
        """
        if pq:
            if isinstance(rule, dict):
                if 'filter' in rule:
                    if isinstance(rule['filter'], list):
                        for f in rule['filter']:
                            pq = pq.find(f)
                            if pq:
                                break
                    else:
                        pq = pq.find(rule['filter'])
                    if not pq:
                        return None
                    data = self._htmlparse(pq, rule)
                    return data
                elif 'type' in rule:
                    data = self._htmlparse(pq, rule)
                    return data
                else:
                    data = {}
                    for idx in rule:
                        data[idx] = self._filter(pq, rule[idx])
                    return data
            elif isinstance(rule, list):
                for item in rule:
                    data = self._filter(pq, item)
                    if data:
                        return data
            else:
                return rule
        return None

    def _htmlparse(self, pq, rule):
        """
        �则解析
        """
        if 'grep' in rule:
            pq = self._grep(pq, rule['grep'])
        if 'not_' in rule:
            pq = pq.not_(rule['not_'])
        if 'is_' in rule:
            pq = pq.is_(rule['is_'])
        if 'eq' in rule:
            idx = int(rule['eq']) or 0
            if idx < 0:
                idx = pq.length + idx
            pq = pq.eq(idx)
        if 'type' in rule:
            onlyOne = int(rule.get('onlyOne', 1))
            target = rule.get('target', None)
            callback = rule.get('callback', None)
            llimiter = rule.get('llimiter', None)
            rlimiter = rule.get('rlimiter', None)
            proccessFun = getattr(self, '_%s' % str(rule['type']))
            content = proccessFun(pq, target=target, onlyOne=onlyOne, callback=callback, llimiter=llimiter, rlimiter=rlimiter)
            if 'match' in rule and rule['match']:
                redata = pcre2re(rule['match']).search(content)
                if not redata:
                    return None
                if 'mkey' in rule:
                    rst = ((rule['mkey'] in redata.groups()) and redata.group(rule['mkey']) or None)
                    if isinstance(rst, six.string_types):
                        return self.patch_result(rst, rule, None)
                    return rst
                rst = redata.group(1)
                if isinstance(rst, six.string_types):
                    return self.patch_result(rst, rule, None)
                return rst
            if isinstance(content, six.string_types):
                return self.patch_result(rst, rule, None)
            return content
        elif 'item' in rule:
            onlyOne = int(rule.get('onlyOne', 0))
            if onlyOne:
                parser = PyqueryParser(rule['item'], str(pq))
                return parser.parse()
            else:
                data = []
                for i in range(pq.length):
                    parser = PyqueryParser(rule['item'], str(pq.eq(i)))
                    data.append(parser.parse())

                return data
        else:
            return pq

    def _html(self, pq, onlyOne = True, callback = None, target = None, llimiter = None, rlimiter = None):
        """
        获取元素html
        """
        if not isinstance(pq, PyQuery):
            return None
        if pq.length == 0:
            return None
        if onlyOne:
            return callback_result(callback, pq.eq(0).html())
        return [callback_result(callback, PyQuery(f).html()) for f in pq]


    def _text(self, pq, onlyOne = True, callback = None, target = None, llimiter = None, rlimiter = None):
        """
        获取元素文本
        """
        if not isinstance(pq, PyQuery):
            return None
        if pq.length == 0:
            return None
        if onlyOne:
            return callback_result(callback, pq.eq(0).text())
        return [callback_result(callback, PyQuery(f).text()) for f in pq]

    def _attr(self, pq, target, onlyOne = True, callback = None, llimiter = None, rlimiter = None):
        """
        获取元素属性
        """
        if not isinstance(pq, PyQuery):
            return None
        if pq.length == 0:
            return None
        if onlyOne:
            return callback_result(callback, pq.eq(0).attr(target))
        return [callback_result(callback, PyQuery(f).attr(target)) for f in pq]

    def _match(self, pq, onlyOne = True, callback = None, target = None, llimiter = None, rlimiter = None):
        """
        正则匹配元素
        """
        if not isinstance(pq, PyQuery):
            return None
        if pq.length == 0:
            return None
        if not llimiter:
            llimiter = ''
        if not rlimiter:
            rlimiter = ''
        pattern = pcre2re(target)
        if onlyOne:
            for i in range(0, pq.length):
                text = pq.eq(i).outer_html()
                if text:
                    m = pattern.search(text)
                    if m:
                        return callback_result(callback, llimiter + m.group(1) + rlimiter)
        else:
            data = {}
            for i in range(0, pq.length):
                text = pq.eq(i).text()
                if text:
                    m = pattern.search(text)
                    if m:
                        data.append(callback_result(callback, llimiter + m.group(1) + rlimiter))
            return data
        return None

    def _grep(self, root, rule):
        """
        按规则匹配元素
        """
        params = rule.get('params', None)
        match = rule.get('pattern', None)
        def process_fun(el, rule):
            source = self._htmlparse(el, rule)
            if match:
                if source:
                    d = pcre2re(match).search(source)
                    if d:
                        return True
            elif params:
                if params == source:
                    return True
            else:
                if source:
                    return True
            return False
        ellist = [root.eq(i) for i in range(len(root)) if process_fun(root.eq(i), rule)]
        if ellist:
            return PyQuery(ellist)
