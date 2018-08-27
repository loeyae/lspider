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
import copy
import traceback
from cdspider.parser.lib.goose3.extractors import BaseExtractor
from cdspider.parser.lib.time_parser import Parser as TimeParser
from cdspider.libs import utils

KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN = {
    'qq.com': [
        {'attribute': 'name', 'value': '_pubtime', 'content': 'content'},
    ],
    'sohu.com': [
        {'attribute': 'property', 'value': 'og:release_date', 'content': 'content'},
        {'attribute': 'id', 'value': 'news-time', 'content': 'text'},
    ],
    'ifeng.com': [
        {'attribute': 'class', 'value': 'yc_tit', 'leaf': {'tag': 'span', 'content': 'text'},},
        {'attribute': 'class', 'value': 'vTit_crumbs', 'leaf': {'attribute': 'class', 'value': 'data', 'content': 'text'},},
    ],
    'news.cn':[
        {'attribute': 'class', 'value': 'h-info', 'leaf': {'attribute': 'class', 'value': 'h-time', 'content': 'text'}},
    ],
}

KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN = {
    'mp.weixin.qq.com': [
        'var\s+ct\s*=\s*"(\d+)".*?;',
        'var\s+publish_time\s*=\s*"([^"]+)"\s+\|\|\s+"([^"]+)";',
    ],
    'toutiao.com': [
        'time:\s+(?:\'|")([^\'"]+)(?:\'|").*'
    ]
}

class PublishDateExtractor(BaseExtractor):
    def extract(self):
        try:
            custom_rule = self.custom_rule.get('pubtime', {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get('pubtime', {}).get('type', 'text'), target=self.custom_rule.get('pubtime', {}).get('target', 'value'))
                if matched:
                    return TimeParser.timeformat(TimeParser.parser_time(self.correction_result(matched, copy.deepcopy(self.custom_rule))) or self.correction_result(matched, copy.deepcopy(self.custom_rule)))
            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN[fulldomain]))
            if self.domain in KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(KNOWN_PUBLISH_DATE_PATTERN_BY_DOMAIN[self.domain]))
            if known_context_patterns:
                script_nodes = self.parser.getElementsByTag(self.article.raw_doc,
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
                            if data and data[0]:
                                return TimeParser.timeformat(TimeParser.parser_time(data[0]) or data[0])
                known_context_patterns = []

            if fulldomain in KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN[fulldomain]))
            if self.domain in KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(KNOWN_PUBLISH_DATE_TAGS_BY_DOMAIN[self.domain]))
            if known_context_patterns:
                for tags in known_context_patterns:
                    data = self.get_message_by_tag(tags)
                    if data:
                        return TimeParser.timeformat(TimeParser.parser_time(data[0]) or data[0])

            if "article:published_time" in self.article.opengraph:
                return self.article.opengraph["article:published_time"]
            if self.article.schema and "datePublished" in self.article.schema:
                return self.article.schema["datePublished"]
            for known_meta_tag in copy.deepcopy(self.config.known_publish_date_tags):
                # if this is a domain specific config and the current
                # article domain does not match the configured domain,
                # do not use the configured publish date pattern
                if known_meta_tag.domain and known_meta_tag.domain != self.article.domain:
                    continue
                meta_tags = self.parser.getElementsByTag(self.article.raw_doc,
                                                         attr=known_meta_tag.attr,
                                                         value=known_meta_tag.value)
                if meta_tags:
                    data = self.parser.getAttribute(meta_tags[0], known_meta_tag.content)
                    return TimeParser.timeformat(TimeParser.parser_time(data) or data)
        except:
            self.config.logger.error(traceback.format_exc())
        if self.article.top_node is not None:
            parent_node = self.parser.getParent(self.article.top_node)
            if parent_node is not None:
                _d = TimeParser.parser_time(self.parser.outerHtml(parent_node))
                if _d:
                    return TimeParser.timeformat(_d)
        if self.article.final_url:
            _d = TimeParser.parser_time_from_url(self.article.final_url)
            if _d:
                return _d
        return TimeParser.timeformat(TimeParser.parser_time(self.article.raw_html, True))
