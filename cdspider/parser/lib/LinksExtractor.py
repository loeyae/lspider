# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-12 10:25:05
"""
import os
from cdspider.parser.lib.goose3.parsers import Parser
from cdspider.libs import utils
from cdspider.parser import KNOWN_TOP_LINK_PATTERN, KNOWN_DETAIL_URLS_PATTERN, KNOWN_RANDOM_KEYS
from urllib.parse import urljoin, urlparse
import tldextract
import re


class LinksExtractor(object):
    """
    Links Extract
    """

    def __init__(self, url, source = None, links = None):
        subdomain, domain = utils.parse_domain(url)

        self.unique = set()
        self.links = list()
        self.links4domain = list()
        self.links4subdomain = list()
        self.links4other = list()
        self.linksofsubdomain = dict()
        self.subdomain = None
        # if subdomain and subdomain != 'www':
        if subdomain:
            self.subdomain = "%s.%s" % (subdomain, domain)
        self.domain = domain
        self.base = url
        if links:
            self.extend(links)
        if source:
            self.exctract(source)

    def exctract(self, source, errors = 'strict'):
        doc = Parser.fromstring(source, errors = errors)
        items = Parser.getElementsByTag(doc, 'a', attr='href', value="^((?!(?:javascript:).+).)+$")
        for i in items:
            attr = Parser.getAttribute(i, 'href')
            if attr == '#':
                continue
            if attr:
                url = urljoin(self.base, attr.strip())
                if url not in self.unique:
                    self.unique.add(url)
                    title = Parser.getText(i)
                    link = {"url": url, "title": title}
                    self.links.append(link)
                    self.analyze(link)

    def extend(self, links):
        for link in links:
            l = urljoin(self.base, link['url'])
            if l not in self.unique:
                self.unique.add(l)
                link['url'] = l
                self.links.append(link)
                self.analyze(link)

    def get_subdomain(self, link):
        parsed = urlparse(link['url'])
        extracted = tldextract.extract(link['url'])
        # if all((extracted.subdomain, extracted.subdomain != 'www', extracted.subdomain != self.subdomain)):
        if all((extracted.subdomain, extracted.subdomain != self.subdomain)):
            d = "%s.%s" % (extracted.subdomain.split(".").pop(), self.domain)
            url = "%s://%s" % (parsed.scheme, d)
            key = utils.md5(d)
            if key in self.linksofsubdomain:
                if parsed.path == '/' or not parsed.path:
                    self.linksofsubdomain[key]['title'] = link['title']
            else:
                self.linksofsubdomain[key] = {"url": url, "title": link['title']}

    def analyze(self, link):
        if self.subdomain:
            if utils.url_is_from_any_domain(link['url'], [self.subdomain]):
                self.links4subdomain.append(link)
            elif utils.url_is_from_any_domain(link['url'], [self.domain]):
                self.get_subdomain(link)
                self.links4domain.append(link)
            else:
                self.links4other.append(link)
        else:
            if utils.url_is_from_any_domain(link['url'], [self.domain]):
                self.get_subdomain(link)
                self.links4domain.append(link)
                self.links4subdomain.append(link)
            else:
                self.links4other.append(link)

    @property
    def infos(self):
        return {'all': self.links, 'domain': self.links4domain, 'subdomain': self.links4subdomain, 'other': self.links4other, 'subdomains': self.links4subdomain + [v for v in self.linksofsubdomain.values()]}


class TopLinkDetector(object):

    matched = set()
    mistaken = set()

    def __init__(self, matched=[], mistaken=[]):
        if matched:
            for item in matched:
                self.matched.add(item)
        if mistaken:
            for item in mistaken:
                self.mistaken.add(item)

    def feed(self, url):
        parsed = urlparse(url)
        extracted = tldextract.extract(url)
        if self.is_root(extracted, parsed):
            return 0.99
        if self.is_sub(extracted, parsed):
            return 0.95
        if self.is_detail(url):
            return 0
        if self.is_top_link(url):
            return 0.99
        score = self.get_path_score(parsed.path)
        return score

    def is_root(self, tldextracted, urlparsed):
        if urlparsed.path == '/' or not urlparsed.path:
            if tldextracted.subdomain == 'www':
                return True
            if not tldextracted.subdomain:
                return True
        return False

    def is_sub(self, tldextracted, urlparsed):
        if urlparsed.path == '/' or not urlparsed.path:
            if tldextracted.subdomain:
                return True
        return False

    def is_detail(self, url):
        for i in KNOWN_DETAIL_URLS_PATTERN:
            if re.findall(i, url):
                return True
        for i in self.mistaken:
            if re.findall(i, url):
                return True
        return False

    def is_top_link(self, url):
        for i in KNOWN_TOP_LINK_PATTERN:
            if re.findall(i, url):
                return True
        for i in self.matched:
            if re.findall(i, url):
                return True
        return False

    def get_path_score(self, path):
        basename = os.path.basename(path)
        pathname = os.path.dirname(path)
        p = pathname.strip('/').split('/')
        l = len(p)
        score = 0.95
        if not basename:
            score -= l * 0.2
            return score
        if re.findall('^(?:index|list)\..+$', basename, re.I):
            score -= 0.01
            return score

class LinkCleaner(object):

    clean_keys = set()
    clean_pattern = set()

    def __init__(self, clean_pattern = None, clean_keys = None):
        for i in KNOWN_RANDOM_KEYS:
            self.clean_keys.add(i)
        if clean_keys:
            for i in clean_keys:
                self.clean_keys.add(i)
        if clean_pattern:
            for i in clean_pattern:
                self.clean_pattern.add(i)

    def clean(self, data):
        if isinstance(data, (list, tuple, set)):
            return [self.clean_url(i) for i in data]
        elif isinstance(data, dict):
            for i in data:
                data[i]["url"] = self.clean_url(data[i]["ulr"])
            return data
        return self.clean_url(data)

    def clean_url(self, url):
        url = utils.build_filter_query(url, exclude=self.clean_keys)
        for i in self.clean_pattern:
            url = re.sub(i, '', url)
        return url

if __name__ == "__main__":
    pass