# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-13 11:36:25
:version: SVN: $Id: catalogue.py 1176 2018-06-14 02:11:39Z zhangyi $
"""

class Catalogue():
    """
    url list
    """
    def __init__(self):
        self._meta_description = ""
        self._meta_lang = ""
        self._meta_favicon = ""
        self._meta_keywords = ""
        self._canonical_link = ""
        self._domain = ""
        self._final_url = ""
        self._link_hash = ""
        self._raw_html = ""
        self._doc = None
        self._raw_doc = None
        self._data = []


    @property
    def domain(self):
        ''' str: Domain of the article parsed

            Note:
                Read only '''
        return self._domain

    @property
    def final_url(self):
        ''' str: The URL that was used to pull and parsed; `None` if raw_html was used

            Note:
                Read only '''
        return self._final_url

    @property
    def link_hash(self):
        ''' str: The MD5 of the final url to be used for various identification tasks

            Note:
                Read only '''
        return self._link_hash

    @property
    def raw_html(self):
        ''' str: The HTML represented as a string

            Note:
                Read only '''
        return self._raw_html

    @property
    def doc(self):
        ''' etree: lxml document that is being processed

            Note:
                Read only '''
        return self._doc

    @property
    def raw_doc(self):
        ''' etree: Original, uncleaned, and untouched lxml document to be processed

            Note:
                Read only '''
        return self._raw_doc

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data.extend(data)

    @property
    def infos(self):
        return self._data
