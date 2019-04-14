# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-7-6 11:24:08
:version: SVN: $Id: attach.py 2296 2018-07-06 08:11:05Z zhangyi $
"""
import traceback
from urllib.parse import urljoin
from cdspider.parser.lib.goose3.extractors import BaseExtractor
from cdspider.parser.lib.goose3.utils.attach import AttachUtils


class AttachExtractor(BaseExtractor):

    def __init__(self, fetcher, config, article):
        super(AttachExtractor, self).__init__(config, article)

        self.fetcher = fetcher

    def extract(self):
        attaches = []
        try:
            custom_rule = self.custom_rule.get('attaches', {}).get('filter') if self.custom_rule else None
            if custom_rule:
                matched = self.custom_match(custom_rule, dtype=self.custom_rule.get('attaches', {}).get('type', 'attr'), target=self.custom_rule.get('attaches', {}).get('target', 'href'), onlyOne = False)
                if matched:
                    download = self.custom_rule.get('attaches', {}).get('download', False)
                    if download:
                        for item in matched:
                            item = urljoin(self.article.final_url or self.config.final_url, item)
                            local_path = AttachUtils.store_attach(self.fetcher, self.article.link_hash, item, self.config)
                            if local_path:
                                attaches.append(local_path)
                    else:
                        attaches.extend(matched)
        except:
            self.config.logger.error(traceback.format_exc())
        return attaches
