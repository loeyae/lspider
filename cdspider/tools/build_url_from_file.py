#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-10 16:33:14
"""

import os
from cdspider.tools import Base

class build_url_from_file(Base):
    """
    build url form file
    """

    def process(self, *args, **kwargs):
        assert len(args) > 1, 'Please input sid'
        assert len(args) > 2, 'Please input file path'
        sid = int(args[0])
        fpath = args[1]
        if not sid:
            raise Exception('Site not exists')
        site = self.g['db']['SitsDB'].get_detail(sid)
        if not site:
            raise Exception('Site not exists')
        if not os.path.isfile(fpath):
            raise Exception('File not exists')

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
            'main_process': site['main_process'],
            'sub_process': site['sub_process'],
            'unique': None,
            'ctime': int(time.time()),
            'utime': 0,
            'creator': site['creator'],
            'updator': site['updator'],
        }
        with open(fpath, 'r') as f:
            line = f.readline()
            i = 1
            while line:
                self.g['logger'].info('current line: %s', i)
                title, url = re.split(r'\t', line)
                urls['title'] = title
                urls['url'] = url
                uid = UrlsDB.insert(urls)
                self.g['logger'].info('inserted urls: %s', uid)
                i += 1
                line = f.readline()
