#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-25 9:55:21
:version: SVN: $Id: XmlParser.py 2079 2018-07-03 11:24:33Z zhangyi $
"""
import lxml.etree
from . import BaseParser
from cdspider.libs.utils import callback_result, decode

class XmlParser(BaseParser):
    """
    XML Parser
    """

    def __init__(self, *args, **kwargs):
        super(XmlParser, self).__init__(*args, **kwargs)

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
                source = str(source)
#        self.logger.info("Xml source: %s" % re.sub(r"(\r|\n|\s{2,})", "", str(source)))
#        self.logger.info("Xml ruleset: %s" % str(ruleset))
        try:
            root  = lxml.etree.fromstring(source)
        except:
            return None
        return self._xmlfilter(root, ruleset)

    def _xmlfilter(self, root, ruleset):
        if 'filter' in ruleset:
            f = ruleset.pop('filter')
            if f.startswith('@xml:'):
                f = f[5:]
            if not f:
                return None
            nodes = root.xpath(f)
        else:
            nodes = root
        if 'item' in ruleset and ruleset['item']:
            onlyOne = int(ruleset.get('onlyOne', 0))
        else:
            onlyOne = int(ruleset.get('onlyOne', 1))
        if onlyOne:
            idx = ruleset.get('eq', 0)
            return self._xmlproprty(nodes[idx], ruleset)
        else:
            data = []
            for node in nodes:
                data.append(self._xmlproprty(node, ruleset))
            return data

    def _xmlproprty(self, node, ruleset):
        if 'item' in ruleset and ruleset['item']:
            return self._xmlfilter(node, ruleset['item'])
        ruleset.setdefault('type', 'text')
        callback = ruleset.get('callback', None)
        if ruleset['type'] == 'text':
            rst = callback_result(callback, node.text)
            if patch:
                return re.sub('\[\w+\]', str(rst), patch)
            return "%s%s%s" %(prefix, rst, suffix)
        elif ruleset['type'] == 'attr':
            assert 'target' in ruleset and ruleset['target'], "Invalid rule setting: target"
            return self.patch_result(node.attrib[ruleset['target']], ruleset, callback)
        elif ruleset['type'] == 'tag':
            return self.patch_result(node.tag, ruleset, callback)
        elif ruleset['type'] == 'values':
            return self.patch_result(node.values, ruleset, callback)
