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
import traceback
from cdspider.libs.goose3.extractors import BaseExtractor
from cdspider.libs import utils

KNOWN_META_AUTHOR_TAGS = [
    {'attribute': 'itemprop', 'value': 'author', 'content': 'text'},
    {'attribute': 'property', 'value': 'article:author', 'content': 'content'},
]
KNOWN_AUTHOR_TAGS_BY_DOMAIN = {
    'sohu.com': [
        {'attribute': 'name', 'value': 'mediaid', 'content': 'content'},
    ],
    '163.com': [
        {'attribute': 'class', 'value': 'source', 'leaf': {'attribute': 'id', 'value': 'source', 'content': 'text'}},
    ],
    'ifeng.com': [
        {'attribute': 'class', 'value': 'yc_tit', 'leaf': {'tag': 'a', 'content': 'text'}}
    ],
    'news.cn':[
        {'attribute': 'class', 'value': 'h-info', 'leaf': {'attribute': 'id', 'value': 'source', 'content': 'text'}},
    ]
}
KNOWN_AUTHOR_PATTERN_BY_DOMAIN = {
    'qq.com': [
        '"media"\s*:\s*"([^"]+)"',
    ],
    'mp.weixin.qq.com': [
        'var\s+nickname\s*=\s*"([^"]+)";'
    ],
    'toutiao.com': [
        'source:\s+(?:\'|")([^\'"]+)(?:\'|").*'
    ]
}

class AuthorsExtractor(BaseExtractor):

    def extract(self):
        authors = []
        try:
            custom_rule = self.custom_rule.get('author', {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get('author', {}).get('type', 'text'), target=self.custom_rule.get('author', {}).get('target', 'value'))
                if matched:
                    authors.extend([matched])
                    return authors
            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in KNOWN_AUTHOR_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_AUTHOR_PATTERN_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_AUTHOR_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_AUTHOR_PATTERN_BY_DOMAIN[self.domain])
            if known_context_patterns:
                script_nodes = self.parser.getElementsByTag(self.article.doc,
                                                            tag='script')
                for script_node in script_nodes:
                    script = self.parser.getText(script_node)
                    if script:
                        rule = '|'.join(known_context_patterns)
                        matched = re.findall(rule, script, re.M)
                        if matched:
                            for i in matched:
                                authors.extend(i)
                            authors = utils.filter(authors)
                            return authors
                known_context_patterns = []

            if fulldomain in KNOWN_AUTHOR_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_AUTHOR_TAGS_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_AUTHOR_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_AUTHOR_TAGS_BY_DOMAIN[self.domain])
            if known_context_patterns:
                for tags in known_context_patterns:
                    data = self.get_message_by_tag(tags)
                    if data:
                        authors.extend(data)
                        return authors

            for tags in KNOWN_META_AUTHOR_TAGS:
                data = self.get_message_by_tag(tags)
                if data:
                    authors.extend(data)
                    return authors

        except:
            self.config.logger.error(traceback.format_exc())
        return list(set(authors))
