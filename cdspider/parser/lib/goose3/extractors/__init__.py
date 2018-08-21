# -*- coding: utf-8 -*-
"""\
This is a python port of "Goose" orignialy licensed to Gravity.com
under one or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.

Python port was written by Xavier Grangier for Recrutae

Gravity.com licenses this file
to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
import lxml
from cdspider.libs.utils import rule2pattern, patch_result, extract_result

class BaseExtractor(object):

    def __init__(self, config, article):
        # config
        self.config = config

        # parser
        self.parser = self.config.get_parser()

        # article
        self.article = article

        # stopwords class
        self.stopwords_class = config.stopwords_class

        self.domain = config.domain
        self.subdomain = config.subdomain

        self.custom_rule = config.custom_rule or {}

    def get_message_by_tag(self, tags, doc = None):
        if doc is None:
            doc = self.article.doc if len(self.article.doc) > 0 else self.article._raw_doc
        if isinstance(doc, list) and doc:
            doc = doc.pop(0)
        leaf = tags.pop('leaf', None)
        content = tags.pop('content', None)
        if 'attribute' in tags:
            tags['attr'] = tags.pop('attribute')
        matched_tags = self.parser.getElementsByTag(doc, **tags)
        data = []
        if matched_tags:
            for matched_tag in matched_tags:
                if leaf:
                    item = self.get_message_by_tag(leaf, matched_tag)
                    if item:
                        data.extend(item)
                elif content:
                    if content == 'text':
                        data.append(self.parser.getText(matched_tag))
                    else:
                        data.append(self.parser.getAttribute(matched_tag, content))
        return data

    def custom_match_elements(self, custom_rule, onlyOne = True, doc = None):
        ret = []
        if doc is None:
            doc = self.article.doc
        if isinstance(doc, list) and doc:
            doc = doc.pop(0)
        if doc is None:
            return None
        if custom_rule.startswith('@css:'):
            custom_rule = custom_rule[5:]
            custom_rule_arr = custom_rule.split(":eq")
            rule = custom_rule_arr.pop(0)
            ret = self.parser.css_select(doc, rule)
            while custom_rule_arr:
                raw_rule = custom_rule_arr.pop(0)
                idx = raw_rule.index(')')
                eq = int(raw_rule[1:idx])
                if isinstance(ret, (list, tuple)) and len(ret) > eq:
                    ret = ret[eq]
                else:
                    return None
                rule = raw_rule[(idx+1):].strip()
                if rule:
                    ret = self.parser.css_select(ret, rule)

        elif custom_rule.startswith('@pq:'):
            custom_rule = custom_rule[4:]
            ret = self.parser.css_select(doc, custom_rule)

        elif custom_rule.startswith('@reg:'):
            custom_rule = custom_rule[5:]
            custom_rule, key = rule2pattern(custom_rule)
            if not custom_rule or not key:
                return ret
            matched = re.findall(custom_rule, self.article.raw_html, re.S|re.I)
            ret = [self.parser.fromstring(item if item.startswith('<') else "<div>%s</div>" % item) for item in matched if item]
        else:
            if custom_rule.startswith('@xpath:'):
                custom_rule = custom_rule[7:]
            ret = self.parser.xpath_re(doc, custom_rule)

        if not ret:
            return None
        if onlyOne:
            return ret[0]
        return ret

    def custom_match(self, custom_rule, onlyOne = True, dtype='text', target=None, doc = None):
        if doc is None:
            doc = self.article.doc
        if isinstance(doc, list) and doc:
            doc = doc.pop(0)
        if doc is None:
            return None
        if custom_rule.startswith('@css:'):
            custom_rule = custom_rule[5:]
            ret = self.parser.css_select(doc, custom_rule)
            if not ret:
                return None
            if onlyOne:
                return self.f(ret[0], dtype, target) if isinstance(ret, (list, tuple)) else self.f(ret, dtype, target)
            return [self.f(item, dtype, target) for item in ret] if isinstance(ret, (list, tuple)) else self.f(ret, dtype, target)
        elif custom_rule.startswith('@value:'):
            ret = custom_rule[7:]
            return ret
        elif custom_rule.startswith('@reg:'):
            custom_rule = custom_rule[5:]
            rule, key = rule2pattern(custom_rule)
            if not rule or not key:
                return None
            matched = None
            if onlyOne:
                r = re.search(rule, str(self.article.raw_html), re.S|re.I)
                if r:
                    matched = r.group(key)
                    if matched:
                        matched = matched.strip()
            else:
                matched = re.findall(rule, str(self.article.raw_html), re.S|re.I)
            return matched
        else:
            if custom_rule.startswith('@xpath:'):
                custom_rule = custom_rule[7:]
            ret = self.parser.xpath_re(doc, custom_rule)
            if not ret:
                return None
            if onlyOne:
                return self.f(ret[0], dtype, target) if isinstance(ret, (list, tuple)) else self.f(ret, dtype, target)
            return [self.f(item, dtype, target) for item in ret] if isinstance(ret, (list, tuple)) else self.f(ret, dtype, target)

    def correction_result(self, data, rule, callback=None):
        return self.patch_result(self.extract_result(data, rule, callback), rule, None)

    def patch_result(self, data, rule, callback=None):
        return patch_result(data, rule, callback)

    def extract_result(self, data, rule, callback=None):
        return extract_result(data, rule, callback)

    def f(self, doc, dtype, target=None):
        if isinstance(doc, lxml.html.HtmlElement):
            if dtype == 'html':
                doc = self.parser.nodeToString(doc)
            elif dtype == 'attr':
                doc = self.parser.getAttribute(doc, target)
            else:
                doc = self.parser.getText(doc)
        return doc
