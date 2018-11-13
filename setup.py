#! /usr/bin/python
#-*- coding: UTF-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

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
        'pymongo>=3.6.0',
        'pika>=0.11.2',
        'tornado>=3.2,<=4.5.3',
        'flask>=0.12.2',
        'flask_login>=0.4.1',
        'Pillow>=5.0.0',
        'cssselect>=1.0.1',
        'jieba3k>=0.35.1',
        'jieba>=0.39.0',
        'beautifulsoup4>=4.6.0',
        'tld>=0.7.9',
        'pykafka>=2.7.0',
        'pycurl>=7.43.0.0',
        'itchat>=1.3.10',
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
            'config/aiml/*.aiml',
            'config/aiml/*.xml',
            "parser/lib/goose3/resources/images/*.txt",
            "parser/lib/goose3/resources/text/*.txt",
        ],
    },

#    include_package_data = True,

#    exclude_package_data = { '': ['README.md'] },
    entry_points = {
        'console_scripts': [
            'cdspider = cdspider.run:main',
        ],
        'cdspider.crawler': [
            'tornado=cdspider.crawler:TornadoCrawler',
            'requests=cdspider.crawler:RequestsCrawler',
            'selenium=cdspider.crawler:SeleniumCrawler',
        ],
        'cdspider.robots': [
            'wxchat=cdspider.robots:WxchatRobots',
        ],
        'cdspider.parser': [
            'list=cdspider.parser:ListParser',
            'item=cdspider.parser:ItemParser',
            'custom=cdspider.parser:CustomParser',
        ],
        'cdspider.queue': [
            'amqp=cdspider.message_queue:AmqpQueue',
            'pika=cdspider.message_queue:PikaQueue',
            'redis=cdspider.message_queue:RedisQueue',
            'kafka=cdspider.message_queue:KafkaQueue',
        ],
        'cdspider.db': [
            'mongo=cdspider.connector:Mongo',
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
