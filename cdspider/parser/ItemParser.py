# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-5-17 14:27:33
"""
import re
from cdspider.libs import utils
from . import BaseParser
from .lib import Goose
from .lib import JsonParser
from .lib.goose3.text import StopWordsChinese


class ItemParser(BaseParser):
    """
    详情页自动解析器
    """
    def __init__(self, *args, **kwargs):
       super(ItemParser, self).__init__(*args, **kwargs)

    def parse(self, source=None, ruleset=None):
        if not source:
            source = self.source
        if not ruleset:
            ruleset = self.ruleset
        if not ruleset:
            ruleset = {}
        item_ruleset = dict(
            [(k, item) for k, item in ruleset.items()
             if item and isinstance(item, dict) and 'filter' in item and item['filter']]
        ) if ruleset else {}
        if item_ruleset:
            rule = list(item_ruleset.values())[0]
            if 'filter' in rule and rule['filter'] and rule['filter'].startswith('@json:'):
                parser = JsonParser(
                    source=source, ruleset=item_ruleset, logger=self.logger, domain=self.domain,
                    subdomain=self.subdomain)
                return parser.parse()
        local_storage_path = self._settings.get('attach_storage', None)
        g = Goose({
            "target_language": "zh", 'stopwords_class': StopWordsChinese, "enable_fewwords_paragraphs": True,
            "logger": self.logger, "domain": self.domain, "subdomain": self.subdomain,
            "custom_rule": item_ruleset if item_ruleset else {}, "local_storage_path": local_storage_path,
            "final_url": self.final_url})

        if isinstance(source, bytes):
            try:
                article = g.extract(raw_html=utils.decode(source), encoding='UTF8')
            except UnicodeDecodeError:
                article = g.extract(raw_html=source)
        else:
            article = g.extract(raw_html=source, encoding='UTF8')
        data = {}
        for i in ruleset:
            if i == 'title':
                data[i] = article.infos['title']['clean_title']
            elif i == 'content':
                data[i] = article.infos['cleaned_text']
                data["raw_content"] = '\r\n'.join(article.top_node_html) if isinstance(article.top_node_html, (list, tuple)) else article.top_node_html
                data["raw_content"] = re.sub('[\u3000\xa0]', ' ', str(data["raw_content"]))
            elif i == 'pubtime':
                data[i] = article.infos['publish_date']
            elif i == 'author':
                data[i] = self.get_author(article.infos['authors'])
            else:
                data[i] = article.infos.get(i, None)
        if 'title' not in data:
            data['title'] = article.infos['title']['clean_title']
        if 'content' not in data:
            data['content'] = article.infos['cleaned_text']
            data["raw_content"] = '\r\n'.join(article.top_node_html) if isinstance(article.top_node_html, (list, tuple)) else article.top_node_html
            data["raw_content"] = re.sub('[\u3000\xa0]', ' ', str(data["raw_content"]))
        if 'pubtime' not in data:
            data['pubtime'] = article.infos['publish_date']
        if 'author' not in data:
            data['author'] = self.get_author(article.infos['authors'])
        return data

    def get_author(self, authors):
        author = ''
        if isinstance(authors, (list, tuple)):
            for item in authors:
                if author == '':
                    author = item
                elif len(item) < len(author):
                    author = item
        else:
            author = authors
        return author
