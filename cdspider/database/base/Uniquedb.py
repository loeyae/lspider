#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:42:16
:version: SVN: $Id: Uniquedb.py 2431 2018-07-31 01:30:53Z zhangyi $
"""

{
    'uniqueue': {
        'unid': str,         # unique str
        'createtime': int,   # 创建时间
    }
}


from cdspider.libs import utils

class UniqueDB(object):

    def insert(self, obj, projectid, taskid, urlid, attachid, kwid, createtime):
        raise NotImplementedError

    def build(self, obj):
        if isinstance(obj, dict):
            return utils.md5(utils.url_encode(obj))
        elif isinstance(obj, list):
            return utils.md5("".join(obj))
        else:
            return utils.md5(str(obj))
