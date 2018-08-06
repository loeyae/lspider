#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-13 11:02:33
:version: SVN: $Id: custom.py 1511 2018-06-25 07:38:26Z zhangyi $
"""
import re
import traceback
from cdspider.parser.lib.goose3.extractors import BaseExtractor
from cdspider.libs import utils


class CustomExtractor(BaseExtractor):
    """
    custom extractor
    """

    KNOWN_CUSTOM_TAGS = [
    ]


    KNOWN_CUSTOM_TAGS_BY_DOMAIN = {
    }

    KNOWN_CUSTOM_PATTERN_BY_DOMAIN = {
    }

    def extract(self, key):
        try:
            onlyOne = bool(int(self.custom_rule.get(key, {}).get('onlyOne', 1)))
            custom_rule = self.custom_rule.get(key, {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get(key, {}).get('type', 'text'), target=self.custom_rule.get(key, {}).get('target', 'value'), onlyOne=onlyOne)
                if matched:
                    return matched
            self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN.update(self.custom_rule.get(key, {}).get('pattern4domain', {}))
            self.KNOWN_CUSTOM_TAGS_BY_DOMAIN.update(self.custom_rule.get(key, {}).get('tags4domain', {}))
            self.KNOWN_CUSTOM_TAGS.extend(self.custom_rule.get(key, {}).get('tags', []))

            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN[fulldomain])
            if self.domain in self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN[self.domain])
            if known_context_patterns:
                script_nodes = self.parser.getElementsByTag(self.article.doc,
                                                            tag='script')
                for script_node in script_nodes:
                    script = self.parser.getText(script_node)
                    if script:
                        rule = '|'.join(known_context_patterns)
                        matched = re.findall(rule, script, re.M)
                        if matched:
                            if onlyOne:
                                return matched[0]
                        return matched
                known_context_patterns = []

            if fulldomain in self.KNOWN_CUSTOM_TAGS_BY_DOMAIN:
                known_context_patterns.extend(self.KNOWN_CUSTOM_TAGS_BY_DOMAIN[fulldomain])
            if self.domain in self.KNOWN_CUSTOM_TAGS_BY_DOMAIN:
                known_context_patterns.extend(self.KNOWN_CUSTOM_TAGS_BY_DOMAIN[self.domain])
            if known_context_patterns:
                for tags in known_context_patterns:
                    matched = self.get_message_by_tag(tags)
                    if matched:
                        if onlyOne:
                            return matched[0]
                    return matched

            for tags in self.KNOWN_CUSTOM_TAGS:
                matched = self.get_message_by_tag(tags)
                if matched:
                    if onlyOne:
                        return matched[0]
                    return matched

        except:
            self.config.logger.error(traceback.format_exc())
        return None
