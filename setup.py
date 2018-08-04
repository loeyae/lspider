#! /usr/bin/python
#-*- coding: UTF-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: setup.py 2269 2018-07-06 07:03:11Z zhangyi $

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2018-1-9 18:06:30$"

import sys
from setuptools import setup, find_packages

require_packages = [
        'msgpack>=0.5.1',
        'umsgpack>=0.1.0',
        'click>=6.7',
        'tldextract>=2.2.0',
        'stevedore>=1.28.0',
        'six>=1.11.0',
        'redis>=2.10.6',
        'lxml>=4.1.1',
        'requests>=2.12.0',
        'PySocks>-1.6.8',
        'selenium>=3.8.0',
        'pyquery>=1.3.0',
        'amqp>=2.2.2',
        'mysql-connector-python>=8.0.5',
        'pika>=0.11.2',
        'tornado>=3.2,<=4.5.3',
        'flask>=0.12.2',
        'flask_login>=0.4.1',
        'cssselect>=1.0.1',
        'jieba3k>=0.35.1',
        'jieba>=0.39.0',
        'beautifulsoup4>=4.6.0',
        'tld>=0.7.9',
        'pykafka>=2.7.0',
    ]
if sys.platform != 'win32':
    require_packages.append('gssapi>=1.5.0')

setup(
    name = "cdspider",
    version = "0.1",
    description = "Color-Data数据采集框架",
    author = "Zhang Yi",
    author_email = "loeyae@gmail.com",
    url = "http://www.color-data.com/",
    license = "Apache License, Version 2.0",

    install_requires = require_packages,
    packages = find_packages(),
#    package_dir = {'':'src'},
#    py_modules=['run'],

    package_data = {
        'cdspider': [
            'config/logging.conf',
            'config/main.json',
            'config/app.json',
            "webui/static/font/*.*",
            "webui/static/css/*.*",
            "webui/static/images/*.*",
            "webui/static/js/*.*",
            "webui/static/js/*/*.*",
            "webui/static/js/*/*/*.*",
            "webui/static/codemirror/*.*",
            "webui/static/codemirror/*/*.*",
            "webui/static/codemirror/*/*/*.*",
            "webui/static/attach/*.html",
            "webui/templates/*.html",
            "webui/templates/*/*.html",
            "libs/goose3/resources/images/*.txt",
            "libs/goose3/resources/text/*.txt",
        ],
    },

#    include_package_data = True,

#    exclude_package_data = { '': ['README.txt'] },
    entry_points = {
        'console_scripts': [
            'cdspider = cdspider.run:main',
        ],
        'cdspider.crawler': [
            'requests=cdspider.crawler:RequestsCrawler',
            'selenium=cdspider.crawler:SeleniumCrawler',
        ],
        'cdspider.parser': [
            'list=cdspider.parser:ListParser',
            'item=cdspider.parser:ItemParser',
            'regular=cdspider.parser:RegularParser',
            'pyquery=cdspider.parser:PyqueryParser',
            'xpath=cdspider.parser:XpathParser',
            'json=cdspider.parser:JsonParser',
            'xml=cdspider.parser:XmlParser'
        ],
        'cdspider.queue': [
            'amqp=cdspider.message_queue:AmqpQueue',
            'pika=cdspider.message_queue:PikaQueue',
            'redis=cdspider.message_queue:RedisQueue',
        ],
        'cdspider.db.mongo': [
            'base=cdspider.database.mongo:Mongo',
            'admindb=cdspider.database.mongo:AdminDB',
            'projectdb=cdspider.database.mongo:ProjectDB',
            'sitetypedb=cdspider.database.mongo:SitetypeDB',
            'sitedb=cdspider.database.mongo:SiteDB',
            'urlsdb=cdspider.database.mongo:UrlsDB',
            'attachmentdb=cdspider.database.mongo:AttachmentDB',
            'taskdb=cdspider.database.mongo:TaskDB',
            'keywordsdb=cdspider.database.mongo:KeywordsDB',
            'uniquedb=cdspider.database.mongo:UniqueDB',
            'resultdb=cdspider.database.mongo:ResultDB',
        ],
        'cdspider.db.mysql': [
            'base=cdspider.database.mysql:Mysql',
            'admindb=cdspider.database.mysql:AdminDB',
            'projectdb=cdspider.database.mysql:ProjectDB',
            'sitetypedb=cdspider.database.mysql:SitetypeDB',
            'sitedb=cdspider.database.mysql:SiteDB',
            'urlsdb=cdspider.database.mysql:UrlsDB',
            'attachmentdb=cdspider.database.mysql:AttachmentDB',
            'taskdb=cdspider.database.mysql:TaskDB',
            'keywordsdb=cdspider.database.mysql:KeywordsDB',
            'uniquedb=cdspider.database.mysql:UniqueDB',
            'resultdb=cdspider.database.mysql:ResultDB',
        ],
        'cdspider.handler': [
            'search=cdspider.handler:SearchHandler',
            'general=cdspider.handler:GeneralHandler',
            'attach=cdspider.handler:AttachHandler',
        ],
        'cdspider.mailer': [
            'smtp=cdspider.mailer:SmtpSender'
        ]
    }
)
