# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-13 11:02:33
:version: SVN: $Id: custom.py 1511 2018-06-25 07:38:26Z zhangyi $
"""
import re
import copy
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

    def extract(self, key, custom_rule = None, onlyOne = None):
        try:
            if custom_rule is None:
                custom_rule = self.custom_rule.get(key, {})
            if onlyOne is None:
                onlyOne = bool(int(custom_rule.get('onlyOne', 1)))
            rule = custom_rule.get('filter', None)
            if rule:
                matched = self.custom_match(rule, dtype=custom_rule.get('type', 'text'), target=custom_rule.get('target', 'value'), onlyOne=onlyOne)
                if matched:
                    return self.correction_result(matched, copy.deepcopy(custom_rule), custom_rule.get('callback'))
            self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN.update(custom_rule.get('pattern4domain', {}))
            self.KNOWN_CUSTOM_TAGS_BY_DOMAIN.update(custom_rule.get('tags4domain', {}))
            self.KNOWN_CUSTOM_TAGS.extend(custom_rule.get('tags', []))

            known_context_patterns = []
            fulldomain = "%s.%s" % (self.subdomain, self.domain)
            if fulldomain in self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN[fulldomain]))
            if self.domain in self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(self.KNOWN_CUSTOM_PATTERN_BY_DOMAIN[self.domain]))
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
                            if onlyOne:
                                return self.correction_result(data[0], custom_rule, custom_rule.get('callback'))
                            return self.correction_result(data, custom_rule, custom_rule.get('callback'))
                known_context_patterns = []

            if fulldomain in self.KNOWN_CUSTOM_TAGS_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(self.KNOWN_CUSTOM_TAGS_BY_DOMAIN[fulldomain]))
            if self.domain in self.KNOWN_CUSTOM_TAGS_BY_DOMAIN:
                known_context_patterns.extend(copy.deepcopy(self.KNOWN_CUSTOM_TAGS_BY_DOMAIN[self.domain]))
            if known_context_patterns:
                for tags in known_context_patterns:
                    matched = self.get_message_by_tag(tags)
                    if matched:
                        if onlyOne:
                            return self.correction_result(matched[0], custom_rule, custom_rule.get('callback'))
                    return self.correction_result(matched, custom_rule, custom_rule.get('callback'))

            for tags in self.KNOWN_CUSTOM_TAGS:
                matched = self.get_message_by_tag(tags)
                if matched:
                    if onlyOne:
                        return self.correction_result(matched[0], custom_rule, custom_rule.get('callback'))
                    return self.correction_result(matched, custom_rule, custom_rule.get('callback'))

        except:
            self.config.logger.error(traceback.format_exc())
        return None
