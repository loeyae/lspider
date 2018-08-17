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
from cdspider.parser.lib.goose3.extractors import BaseExtractor
from cdspider.parser.lib.time_parser import Parser as TimeParser
from cdspider.libs import utils

KNOWN_PUBLISH_DATE_TAGS = [
    {'attribute': 'property', 'value': 'rnews:datePublished', 'content': 'content'},
    {'attribute': 'property', 'value': 'article:published_time', 'content': 'content'},
    {'attribute': 'name', 'value': 'OriginalPublicationDate', 'content': 'content'},
    {'attribute': 'itemprop', 'value': 'datePublished', 'content': 'datetime'},
    {'attribute': 'class', 'value': 'publish_time', 'content': 'text'},
]

KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN = {
    'qq.com': [
        {'attribute': 'name', 'value': '_pubtime', 'content': 'content'},
    ],
    'sohu.com': [
        {'attribute': 'property', 'value': 'og:release_date', 'content': 'content'},
    ],
    'ifeng.com': [
        {'attribute': 'class', 'value': 'yc_tit', 'leaf': {'tag': 'span', 'content': 'text'},},
    ],
    'news.cn':[
        {'attribute': 'class', 'value': 'h-info', 'leaf': {'attribute': 'class', 'value': 'h-time', 'content': 'text'}},
    ]
}

KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN = {
    'mp.weixin.qq.com': [
        'var\s+ct\s*=\s*"(\d+)".*?;'
        'var\s+publish_time\s*=\s*"([^"]+)"\s+\|\|\s+"([^"]+)";',
    ],
    'toutiao.com': [
        'time:\s+(?:\'|")([^\'"]+)(?:\'|").*'
    ]
}

class PublishDateExtractor(BaseExtractor):
    def extract(self):
        try:
            custom_rule = self.custom_rule.get('created', {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get('created', {}).get('type', 'text'), target=self.custom_rule.get('created', {}).get('target', 'value'))
                if matched:
                    return TimeParser.timeformat(TimeParser.parser_time(self.correction_result(matched)))
            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN[self.domain])
            if known_context_patterns:
                script_nodes = self.parser.getElementsByTag(self.article.doc,
                                                            tag='script')
                for script_node in script_nodes:
                    script = self.parser.getText(script_node)
                    if script:
                        rule = '|'.join(known_context_patterns)
                        matched = re.findall(rule, script, re.M)
                        if matched:
                            data = []
                            for i in matched:
                                if isinstance(i, (list, tuple)):
                                    data.extend(i)
                                else:
                                    data.append(i)
                            data = utils.filter(data)
                            return TimeParser.timeformat(TimeParser.parser_time(data[0]))
                known_context_patterns = []

            if fulldomain in KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN[self.domain])
            if known_context_patterns:
                for tags in known_context_patterns:
                    data = self.get_message_by_tag(tags)
                    if data:
                        return TimeParser.timeformat(TimeParser.parser_time(data[0]))

            for tags in KNOWN_PUBLISH_DATE_TAGS:
                data = self.get_message_by_tag(tags)
                if data:
                    return TimeParser.timeformat(TimeParser.parser_time(data[0]))
        except:
            self.config.logger.error(traceback.format_exc())

        return TimeParser.timeformat(TimeParser.parser_time(self.article.raw_html, True))
