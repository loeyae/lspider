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
from cdspider.libs import utils


TITLE_SPLITTERS = ["|", "-", "»", "_"]

KNOWN_TITLE_TAGS = [
    {'tag': 'meta', 'attribute': 'name', 'value': 'headline', 'content': 'content'},
]

KNOWN_TITLE_TAGS_BY_DOMAIN = {
    'sohu.com': [
        {'tag': 'meta', 'attribute': 'property', 'value': 'og:title', 'content': 'content'},
    ],
    'sina.com.cn': [
        {'tag': 'meta', 'attribute': 'property', 'value': 'og:title', 'content': 'content'},
    ],
    '163.com': [
        {'tag': 'div', 'attribute': 'class', 'value': 'post_content_main', 'leaf': {'tag': 'h1', 'content': 'text'}},
    ],
    'ifeng.com': [
        {'tag': 'div', 'attribute': 'yc_tit', 'value': 'post_content_main', 'leaf': {'tag': 'h1', 'content': 'text'}},
    ]
}

KNOWN_TITLE_PATTERN_BY_DOMAIN = {
    'mp.weixin.qq.com': [
        r'var\s+msg_title\s*=\s*"([^"]+)";'
    ],
    'toutiao.com': [
        'title:\s+(?:\'|")([^\'"]+)(?:\'|").*'
    ]
}

class TitleExtractor(BaseExtractor):

    def clean_title(self, title):
        """Clean title with the use of og:site_name
        in this case try to get rid of site name
        and use TITLE_SPLITTERS to reformat title
        """
        # check if we have the site name in opengraph data
        if "site_name" in list(self.article.opengraph.keys()):
            site_name = self.article.opengraph['site_name']
            # remove the site name from title
            title = title.replace(site_name, '').strip()

        # try to remove the domain from url
        if self.article.domain:
            pattern = re.compile(self.article.domain, re.IGNORECASE)
            title = pattern.sub("", title).strip()

        # split the title in words
        # TechCrunch | my wonderfull article
        # my wonderfull article | TechCrunch
        # check for an empty title
        # so that we don't get an IndexError below
        title_words = title.split()
        if len(title_words) == 0:
            return ""

        # check if first letter is in TITLE_SPLITTERS
        # if so remove it
        if title_words[0] in TITLE_SPLITTERS:
            title_words.pop(0)

        # check if last letter is in TITLE_SPLITTERS
        # if so remove it
        if title_words[-1] in TITLE_SPLITTERS:
            title_words.pop(-1)

        # rebuild the title
        title = " ".join(title_words).strip()

        return title

    def get_title(self):
        """
        Fetch the article title and analyze it
        """
        title = ''
        try:
            custom_rule = self.custom_rule.get('title', {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get('title', {}).get('type', 'text'), target=self.custom_rule.get('title', {}).get('target', 'value'))
                if matched:
                    return {'raw_title': matched, 'clean_title': self.clean_title(matched)}

            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in KNOWN_TITLE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_TITLE_PATTERN_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_TITLE_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_TITLE_PATTERN_BY_DOMAIN[self.domain])
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
                            return {'raw_title': data[0], 'clean_title': self.clean_title(data[0])}
                known_context_patterns = []

            if fulldomain in KNOWN_TITLE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_TITLE_TAGS_BY_DOMAIN[fulldomain])
            if self.domain in KNOWN_TITLE_TAGS_BY_DOMAIN:
                known_context_patterns.extend(KNOWN_TITLE_TAGS_BY_DOMAIN[self.domain])
            if known_context_patterns:
                for tags in known_context_patterns:
                    data = self.get_message_by_tag(tags)
                    if data:
                        return {'raw_title': data[0], 'clean_title': self.clean_title(data[0])}

            if not fulldomain in KNOWN_TITLE_TAGS_BY_DOMAIN and not self.domain in KNOWN_TITLE_TAGS_BY_DOMAIN:
                # rely on opengraph in case we have the data
                if "title" in list(self.article.opengraph.keys()):
                    title = self.article.opengraph['title']
                    return {'raw_title': title, 'clean_title': self.clean_title(title)}

            for tags in KNOWN_TITLE_TAGS:
                data = self.get_message_by_tag(tags)
                if data:
                    return {'raw_title': data[0], 'clean_title': self.clean_title(data[0])}

            # otherwise use the title meta
            title_element = self.parser.getElementsByTag(self.article.doc, tag='title')
            if title_element is not None and len(title_element) > 0:
                title = self.parser.getText(title_element[0])
                return {'raw_title': title, 'clean_title': self.clean_title(title)}
        except:
            self.config.logger.error(traceback.format_exc())

        return {'raw_title': title, 'clean_title': title}

    def extract(self):
        return self.get_title()
