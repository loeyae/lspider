# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-13 12:55:26
:version: SVN: $Id: catalogue.py 1550 2018-06-25 12:08:40Z zhangyi $
"""
import copy
from cdspider.parser.lib.goose3.extractors import BaseExtractor
from cdspider.libs import utils
from cdspider.parser import KNOWN_DETAIL_URLS_PATTERN

class CatalogueExtractor(BaseExtractor):
    """
    put you comment
    """

    KNOWN_URLS_TOP_TAGS = [
        {'attr': 'class', 'value': 'list'},
        {'attr': 'class', 'value': 'center'},
        {'attr': 'class', 'value': 'main'},
    ]

    KNOWN_URLS_TOP_TAGS_BY_DOMAIN = {

    }

    KNOWN_URLS_TAGS = [
        {"tag": "a", "attribute": "href", "value": "^((?!(?:javascript:[^;]*;|#)).)+$", "content": "href"}
    ]


    KNOWN_URLS_TAGS_BY_DOMAIN = {

    }

    KNOWN_URLS_PATTERN = KNOWN_DETAIL_URLS_PATTERN

    KNOWN_URLS_PATTERN_BY_DOMAIN = {
    }

    def extract(self):
        if self.custom_rule and 'item' in self.custom_rule and self.custom_rule['item']:
            if 'filter' in self.custom_rule and self.custom_rule['filter']:
                doc = self.custom_match_elements(copy.deepcopy(self.custom_rule['filter']), doc=self.article.doc)
            else:
                doc = self.article.doc
            custom_rule = copy.deepcopy(self.custom_rule['item'])
            urls_pattern = custom_rule.pop('url', {})
            data = dict()
            if not urls_pattern:
                for k, rule in custom_rule.items():
                    val = self.custom_match(rule['filter'], onlyOne=False, dtype=rule.get('type', 'text'),
                                            target=rule.get('target', None))
                    data[k] = self.correction_result(val, rule)
                return utils.table2kvlist(data, extend=True)
            if 'filter' in urls_pattern and urls_pattern['filter']:
                if not urls_pattern['filter'].startswith('@'):
                    rule, key = utils.rule2pattern(urls_pattern['filter'])
                    if not key:
                        urls = self.custom_match(
                            urls_pattern['filter'], onlyOne=False, dtype=urls_pattern.get('type','attr'),
                            target=urls_pattern.get('target', 'href'), doc=doc)
                        urls = self.correction_result(urls, urls_pattern)
                    else:
                        urls = self.get_message_by_tag({'tag': 'a', 'attr': 'href', 'value': rule, 'content': 'href'},
                                                       doc=doc)
                        urls = self.correction_result(urls, urls_pattern)
                else:
                    urls = self.custom_match(
                        urls_pattern['filter'], onlyOne=False, dtype=urls_pattern.get('type', 'attr'),
                        target=urls_pattern.get('target', 'href'), doc=doc)
                    urls = self.correction_result(urls, urls_pattern)
                if urls:
                    data = {'url': urls}
                    for k, rule in custom_rule.items():
                        if rule and 'filter' in rule and rule['filter']:
                            val = self.custom_match(
                                rule['filter'], onlyOne=False, dtype=rule.get('type', 'text'),
                                target=rule.get('target', None))
                            data[k] = self.correction_result(val, rule)
                    return utils.table2kvlist(data, extend=True)
                return []

        known_context_patterns = []
        fulldomain = "%s.%s" % (self.subdomain, self.domain)
        if fulldomain in self.KNOWN_URLS_PATTERN_BY_DOMAIN:
            known_context_patterns.extend(copy.deepcopy(self.KNOWN_URLS_PATTERN_BY_DOMAIN[fulldomain]))
        if self.domain in self.KNOWN_URLS_PATTERN_BY_DOMAIN:
            known_context_patterns.extend(copy.deepcopy(self.KNOWN_URLS_PATTERN_BY_DOMAIN[self.domain]))
        if known_context_patterns:
            for rule in known_context_patterns:
                nodes = self.parser.getElementsByTag(self.article.doc, tag='a', attr='href', value=rule)
                if nodes:
                    data = []
                    for node in nodes:
                        data.append({"title": self.parser.getText(node), 'url': self.parser.getAttribute(node, 'href')})
                    return data

        for rule in self.KNOWN_URLS_PATTERN:
            nodes = self.parser.getElementsByTag(self.article.doc, tag='a', attr='href', value=rule)
            if nodes:
                data = []
                for node in nodes:
                    data.append({"title": self.parser.getText(node), 'url': self.parser.getAttribute(node, 'href')})
                return data

        known_context_patterns = []
        if fulldomain in self.KNOWN_URLS_TAGS_BY_DOMAIN:
            known_context_patterns.extend(copy.deepcopy(self.KNOWN_URLS_TAGS_BY_DOMAIN[fulldomain]))
        if self.domain in self.KNOWN_URLS_TAGS_BY_DOMAIN:
            known_context_patterns.extend(copy.deepcopy(self.KNOWN_URLS_TAGS_BY_DOMAIN[self.domain]))
        if known_context_patterns:
            for tags in known_context_patterns:
                data = self.get_message_by_tag(tags, link=True)
                if data:
                    return data

        data = self.auto_match()
        if data:
            return data

        for tags in copy.deepcopy(self.KNOWN_URLS_TAGS):
            data = self.get_message_by_tag(tags, link=True)
            if data:
                return data
        return []

    def auto_match(self):
        top_node = self.calculate_best_node()
        return []

    def calculate_best_node(self):

        doc = self.article.doc
        top_node = None
        return top_node

    def nodes_to_check(self, docs):
        nodes_to_check = []

        for doc in docs:
            items = self.parser.getElementsByTag(doc, tag='a', attr='href', value='^((?!(?:javascript:[^;]*;|#)).)+$')
            nodes_to_check += items
        return nodes_to_check
