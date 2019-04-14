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
import time
import os
import glob
from copy import deepcopy
from cdspider.libs import utils
from cdspider.parser.lib.goose3.article import Article
from cdspider.parser.lib.goose3.catalogue import Catalogue
from cdspider.parser.lib.goose3.utils import URLHelper, RawHelper
from cdspider.parser.lib.goose3.extractors.content import StandardContentExtractor
from cdspider.parser.lib.goose3.extractors.videos import VideoExtractor
from cdspider.parser.lib.goose3.extractors.title import TitleExtractor
from cdspider.parser.lib.goose3.extractors.images import ImageExtractor
from cdspider.parser.lib.goose3.extractors.links import LinksExtractor
from cdspider.parser.lib.goose3.extractors.tweets import TweetsExtractor
from cdspider.parser.lib.goose3.extractors.authors import AuthorsExtractor
from cdspider.parser.lib.goose3.extractors.attach import AttachExtractor
from cdspider.parser.lib.goose3.extractors.tags import TagsExtractor
from cdspider.parser.lib.goose3.extractors.opengraph import OpenGraphExtractor
from cdspider.parser.lib.goose3.extractors.publishdate import PublishDateExtractor
from cdspider.parser.lib.goose3.extractors.metas import MetasExtractor
from cdspider.parser.lib.goose3.extractors.custom import CustomExtractor
from cdspider.parser.lib.goose3.extractors.catalogue  import CatalogueExtractor
from cdspider.parser.lib.goose3.extractors.reportagenewsarticle import ReportageNewsArticleExtractor
from cdspider.parser.lib.goose3.cleaners import StandardDocumentCleaner
from cdspider.parser.lib.goose3.outputformatters import StandardOutputFormatter

from cdspider.parser.lib.goose3.network import NetworkFetcher


class CrawlCandidate(object):
    def __init__(self, config, url, raw_html, encoding=None):
        self.config = config
        # parser
        self.parser = self.config.get_parser()
        self.url = url
        self.raw_html = raw_html
        self.encoding = encoding


class Crawler(object):
    def __init__(self, config, fetcher=None):
        # config
        self.config = config
        # parser
        self.parser = self.config.get_parser()

        # article
        self.article = Article()

        # init the extractor
        self.extractor = self.get_extractor()

        # init the document cleaner
        self.cleaner = self.get_cleaner()

        # init the output formatter
        self.formatter = self.get_formatter()

        # metas extractor
        self.metas_extractor = self.get_metas_extractor()

        # opengraph extractor
        self.opengraph_extractor = self.get_opengraph_extractor()

        # reportage news article extractor
        self.reportagenewsarticle_extractor = self.get_reportagenewsarticle_extractor()

        # publishdate extractor
        self.publishdate_extractor = self.get_publishdate_extractor()

        # tags extractor
        self.tags_extractor = self.get_tags_extractor()

        # authors extractor
        self.authors_extractor = self.get_authors_extractor()

        # tweets extractor
        self.tweets_extractor = self.get_tweets_extractor()

        # links extractor
        self.links_extractor = self.get_links_extractor()

        # video extractor
        self.video_extractor = self.get_video_extractor()

        # title extractor
        self.title_extractor = self.get_title_extractor()

        # html fetcher
        if isinstance(fetcher, NetworkFetcher):
            self.fetcher = fetcher
        else:
            self.fetcher = NetworkFetcher(self.config)

        # attach extractor
        self.attach_extractor = self.get_attach_extractor()

        # image extractor
        self.image_extractor = self.get_image_extractor()

        self.custom_extractor = self.get_custom_extractor()

        # TODO: use the log prefix
        self.log_prefix = "crawler: "

    def crawl(self, crawl_candidate):

        # parser candidate
        parse_candidate = self.get_parse_candidate(crawl_candidate)

        # raw html
        raw_html = self.get_html(crawl_candidate, parse_candidate)

        if raw_html is None:
            return self.article

        return self.process(raw_html, parse_candidate.url, parse_candidate.link_hash, parse_candidate.encoding)

    def process(self, raw_html, final_url, link_hash, encoding=None):

        # create document
        doc = self.get_document(raw_html, encoding)

        # article
        self.article._final_url = final_url or self.config.final_url
        self.article._link_hash = link_hash
        self.article._raw_html = raw_html
        self.article._doc = doc
        self.article._raw_doc = deepcopy(doc)

        # open graph
        self.article._opengraph = self.opengraph_extractor.extract()

        # schema (ReportageNewsArticle) https://pending.schema.org/ReportageNewsArticle
        self.article._schema = self.reportagenewsarticle_extractor.extract()

        if not self.article._final_url:
            if "url" in self.article.opengraph:
                self.article._final_url = self.article.opengraph["url"]
            elif self.article.schema and "url" in self.article.schema:
                self.article._final_url = self.article.schema["url"]

        # meta
        metas = self.metas_extractor.extract()
        # print(metas)
        self.article._meta_lang = metas['lang']
        self.article._meta_favicon = metas['favicon']
        self.article._meta_description = metas['description']
        self.article._meta_keywords = metas['keywords']
        self.article._meta_encoding = metas['encoding']
        self.article._canonical_link = metas['canonical']
        self.article._domain = metas['domain']

        # tags
        self.article._tags = self.tags_extractor.extract()

        # authors
        self.article._authors = self.authors_extractor.extract()

        # title
        self.article._title = self.title_extractor.extract()

        self.article._attaches = self.attach_extractor.extract()

        for k in self.config.custom_rule:
            if k not in ('title', 'author', 'pubtime', 'content', 'attaches'):
                self.article.add_additional_data(k, self.custom_extractor.extract(k))

        # check for known node as content body
        # if we find one force the article.doc to be the found node
        # this will prevent the cleaner to remove unwanted text content
        article_body = self.extractor.get_known_article_tags()
        if article_body is not None:
            doc = article_body

        # before we do any calcs on the body itself let's clean up the document
        if not isinstance(doc, list):
            doc = [self.cleaner.clean(doc)]
        else:
            doc = [self.cleaner.clean(deepcopy(x)) for x in doc]

        # big stuff
        self.article._top_node = self.extractor.custom_top_node(doc)

        if self.article._top_node is not None:
            self.article._doc = doc

            # publishdate
            self.article._publish_date = self.publishdate_extractor.extract()

            # article links
            self.article._links = self.links_extractor.extract()

            # tweets
            self.article._tweets = self.tweets_extractor.extract()

            # video handling
            self.article._movies = self.video_extractor.get_videos()

            # image handling
            if self.config.enable_image_fetching:
                self.get_image()

            self.article._top_node_html = self.parser.outerHtml(self.article._top_node)

            # clean_text
            self.article._cleaned_text = self.formatter.get_formatted_text()
        else:
            self.article._top_node = self.extractor.calculate_best_node(self.article._doc)

            # publishdate
            self.article._publish_date = self.publishdate_extractor.extract()

        # if we have a top node
        # let's process it
        if self.article._top_node is not None:

            # article links
            self.article._links = self.links_extractor.extract()

            # tweets
            self.article._tweets = self.tweets_extractor.extract()

            # video handling
            self.article._movies = self.video_extractor.get_videos()

            # image handling
            if self.config.enable_image_fetching:
                self.get_image()

            # post cleanup
            self.article._top_node = self.extractor.post_cleanup()

            self.article._top_node_html = self.parser.outerHtml(self.article._top_node)

            # clean_text
            self.article._cleaned_text = self.formatter.get_formatted_text()

        if not self.article._cleaned_text:
            self.article._cleaned_text = self.extractor.extract()

        # cleanup tmp file
        self.release_resources()

        # return the article
        return self.article

    @staticmethod
    def get_parse_candidate(crawl_candidate):
        if crawl_candidate.raw_html:
            return RawHelper.get_parsing_candidate(crawl_candidate.url, crawl_candidate.raw_html, crawl_candidate.encoding)
        return URLHelper.get_parsing_candidate(crawl_candidate.url)

    def get_image(self):
        doc = self.article.raw_doc
        top_node = self.article.top_node
        self.article._top_image = self.image_extractor.get_best_image(doc, top_node)

    def get_html(self, crawl_candidate, parsing_candidate):
        # we got a raw_tml
        # no need to fetch remote content
        if crawl_candidate.raw_html:
            return crawl_candidate.raw_html

        # fetch HTML
        html = self.fetcher.fetch(parsing_candidate.url)
        return html

    def get_metas_extractor(self):
        return MetasExtractor(self.config, self.article)

    def get_publishdate_extractor(self):
        return PublishDateExtractor(self.config, self.article)

    def get_opengraph_extractor(self):
        return OpenGraphExtractor(self.config, self.article)

    def get_reportagenewsarticle_extractor(self):
        return ReportageNewsArticleExtractor(self.config, self.article)

    def get_tags_extractor(self):
        return TagsExtractor(self.config, self.article)

    def get_authors_extractor(self):
        return AuthorsExtractor(self.config, self.article)

    def get_attach_extractor(self):
        return AttachExtractor(self.fetcher, self.config, self.article)

    def get_tweets_extractor(self):
        return TweetsExtractor(self.config, self.article)

    def get_links_extractor(self):
        return LinksExtractor(self.config, self.article)

    def get_title_extractor(self):
        return TitleExtractor(self.config, self.article)

    def get_image_extractor(self):
        return ImageExtractor(self.fetcher, self.config, self.article)

    def get_video_extractor(self):
        return VideoExtractor(self.config, self.article)

    def get_formatter(self):
        return StandardOutputFormatter(self.config, self.article)

    def get_cleaner(self):
        return StandardDocumentCleaner(self.config, self.article)

    def get_document(self, raw_html, encoding=None):
        doc = self.parser.fromstring(raw_html, encoding)
        return doc

    def get_extractor(self):
        return StandardContentExtractor(self.config, self.article)

    def get_custom_extractor(self):
        return CustomExtractor(self.config, self.article)


    def release_resources(self):
        if not self.config.local_storage_path:
            return
        path = os.path.join(self.config.local_storage_path, '%s_*' % self.article.link_hash)
        for fname in glob.glob(path):
            try:
                os.remove(fname)
            except OSError:
                # TODO: better log handeling
                pass

class CatalogueCrawler(object):
    def __init__(self, config, fetcher=None):
        # config
        self.config = config
        # parser
        self.parser = self.config.get_parser()

        # catalogue
        self.catalogue = Catalogue()

        # metas extractor
        self.metas_extractor = self.get_metas_extractor()

        # html fetcher
        if isinstance(fetcher, NetworkFetcher):
            self.fetcher = fetcher
        else:
            self.fetcher = NetworkFetcher(self.config)

        # TODO: use the log prefix
        self.log_prefix = "urlcrawler: "

        # metas extractor
        self.metas_extractor = self.get_metas_extractor()

        self.extractor = self.get_extractor()

    def crawl(self, crawl_candidate):

        # parser candidate
        parse_candidate = self.get_parse_candidate(crawl_candidate)

        # raw html
        raw_html = self.get_html(crawl_candidate, parse_candidate)

        if raw_html is None:
            return self.catalogue

        return self.process(raw_html, parse_candidate.url, parse_candidate.link_hash, parse_candidate.encoding)

    def get_document(self, raw_html, encoding=None):
        doc = self.parser.fromstring(raw_html, encoding)
        return doc

    def get_html(self, crawl_candidate, parsing_candidate):
        # we got a raw_tml
        # no need to fetch remote content
        if crawl_candidate.raw_html:
            return crawl_candidate.raw_html

        # fetch HTML
        html = self.fetcher.fetch(parsing_candidate.url)
        return html

    @staticmethod
    def get_parse_candidate(crawl_candidate):
        if crawl_candidate.raw_html:
            return RawHelper.get_parsing_candidate(crawl_candidate.url, crawl_candidate.raw_html, crawl_candidate.encoding)
        return URLHelper.get_parsing_candidate(crawl_candidate.url)

    def get_metas_extractor(self):
        return MetasExtractor(self.config, self.catalogue)

    def get_extractor(self):
        return CatalogueExtractor(self.config, self.catalogue)

    def process(self, raw_html, final_url, link_hash, encoding=None):

        # create document
        doc = self.get_document(raw_html, encoding)

        # catalogue
        self.catalogue._final_url = final_url or self.config.final_url
        self.catalogue._link_hash = link_hash
        self.catalogue._raw_html = raw_html
        self.catalogue._doc = doc
        self.catalogue._raw_doc = deepcopy(doc)

        metas = self.metas_extractor.extract()
        self.catalogue._meta_lang = metas['lang']
        self.catalogue._meta_favicon = metas['favicon']
        self.catalogue._meta_description = metas['description']
        self.catalogue._meta_keywords = metas['keywords']
        self.catalogue._canonical_link = metas['canonical']
        self.catalogue._domain = metas['domain']

        self.catalogue.data = self.extractor.extract()

        return self.catalogue

class CustomCrawler(object):
    def __init__(self, config, fetcher=None):
        # config
        self.config = config
        # parser
        self.parser = self.config.get_parser()

        # catalogue
        self.catalogue = Catalogue()

        # html fetcher
        if isinstance(fetcher, NetworkFetcher):
            self.fetcher = fetcher
        else:
            self.fetcher = NetworkFetcher(self.config)

        # TODO: use the log prefix
        self.log_prefix = "urlcrawler: "

        self.extractor = self.get_extractor()

    def crawl(self, crawl_candidate):

        # parser candidate
        parse_candidate = self.get_parse_candidate(crawl_candidate)

        # raw html
        raw_html = self.get_html(crawl_candidate, parse_candidate)

        if raw_html is None:
            return self.catalogue

        return self.process(raw_html, parse_candidate.url, parse_candidate.link_hash, parse_candidate.encoding)

    def get_document(self, raw_html, encoding=None):
        doc = self.parser.fromstring(raw_html, encoding)
        return doc

    def get_html(self, crawl_candidate, parsing_candidate):
        # we got a raw_tml
        # no need to fetch remote content
        if crawl_candidate.raw_html:
            return crawl_candidate.raw_html

        # fetch HTML
        html = self.fetcher.fetch(parsing_candidate.url)
        return html

    @staticmethod
    def get_parse_candidate(crawl_candidate):
        if crawl_candidate.raw_html:
            return RawHelper.get_parsing_candidate(crawl_candidate.url, crawl_candidate.raw_html, crawl_candidate.encoding)
        return URLHelper.get_parsing_candidate(crawl_candidate.url)

    def get_extractor(self):
        return CustomExtractor(self.config, self.catalogue)

    def process(self, raw_html, final_url, link_hash, encoding=None):

        # create document
        doc = self.get_document(raw_html, encoding)

        # catalogue
        self.catalogue._final_url = final_url or self.config.final_url
        self.catalogue._link_hash = link_hash
        self.catalogue._raw_html = raw_html
        self.catalogue._doc = doc
        self.catalogue._raw_doc = deepcopy(doc)

        custom_rule = self.config.custom_rule
        if custom_rule:
            data = {}
            if 'item' in custom_rule and custom_rule['item']:
                if 'filter' in custom_rule and custom_rule['filter']:
                    doc = self.extractor.custom_match_elements(custom_rule['filter'], doc=doc)
                onlyOne = custom_rule.get('onlyOne', 1)
                self.catalogue._doc = doc
                for key, rule in custom_rule['item'].items():
                    parsed = self.extractor.extract(key, rule, onlyOne)
                    parsed = utils.patch_result(parsed, rule)
                    parsed = utils.extract_result(parsed, rule)
                    data[key] = parsed
                self.catalogue.data = utils.table2kvlist(data)
            else:
                for key, rule in custom_rule.items():
                    parsed = self.extractor.extract(key, rule)
                    parsed = utils.patch_result(parsed, rule)
                    parsed = utils.extract_result(parsed, rule)
                    data[key] = [parsed] if not isinstance(parsed, list) else parsed
                self.catalogue.data = utils.table2kvlist(data)
        return self.catalogue
