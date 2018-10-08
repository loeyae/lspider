#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-10 16:33:14
"""

import os
import re
import time
import copy
from cdspider.tools import Base

class build_url_from_file(Base):
    """
    build url form file
    """

    def process(self, *args, **kwargs):
        sid = int(self.get_arg(args, 0, 'Pleas input sid'))
        fpath = self.get_arg(args, 1, 'Pleas input file path')
        encode = 'utf8'
        if len(args) > 2:
            encode = args[2]
        self.broken('Sitenot exists', sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        self.broken('File: %s not exists' % fpath, os.path.isfile(fpath))
        self.notice('Selected Site Info:', site)

        urlsscript = """
from cdspider.handler.custom.{projectname} import SiteHandler

class UrlHandler(SiteHandler):
    pass
"""
        urlsscript = urlsscript.format(projectname="Project%s" % site['pid'])
        UrlsDB = self.g['db']['UrlsDB']
        urls = {
            'sid': sid,
            'pid': site['pid'],
            'status': UrlsDB.STATUS_INIT,
            'rate': site['rate'],
            'scripts': urlsscript,
            'main_process': None,
            'sub_process': None,
            'unique': None,
            'ctime': int(time.time()),
            'utime': 0,
            'creator': site['creator'],
            'updator': site['updator'],
        }
        self.notice('Built Urls Info:', urls)
        with open(fpath, 'r', encoding=encode) as f:
            line = f.readline()
            i = 1
            while line:
                self.info('current line: %s', i)
                title, url = re.split(r'\t', line)
                item = copy.deepcopy(urls)
                item['name'] = title.strip()
                item['url'] = url.strip()
                uid = UrlsDB.insert(item)
                self.info('inserted urls: %s', uid)
                d={}
                d['uid'] = uid
                self.info("push newtask_queue data: %s" %  str(d))
                self.g['queue']['newtask_queue'].put_nowait(d)
                i += 1
                line = f.readline()
