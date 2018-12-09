#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:36:43
:version: SVN: $Id: Uniquedb.py 2430 2018-07-31 01:30:48Z zhangyi $
"""
from pymongo.errors import *
from cdspider.database.base import CommentsUniqueDB as BaseCommentsUniqueDB
from .Mongo import Mongo, SplitTableMixin

class CommentsUniqueDB(Mongo, BaseCommentsUniqueDB, SplitTableMixin):

    __tablename__ = 'comments_unique'

    def __init__(self, connector, table=None, **kwargs):
        super(CommentsUniqueDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj, ctime):
        unid = self.build(obj)
        table = self._table_name(unid)
        try:
            _id = super(CommentsUniqueDB, self).insert({"unid": unid, "ctime": ctime}, table=table)
            if _id:
                return (True, {'unid': unid, 'ctime': ctime})
            else:
                return (False, False)
        except DuplicateKeyError:
            result = self.get(where={"unid": unid}, table=table)
            return (False, {"unid": result['unid'], "ctime": result['ctime']})
        except:
            return (False, False)

    def _table_name(self, unid):
        return super(CommentsUniqueDB, self)._collection_name(unid[0:1])

    def _check_collection(self):
        self._list_collection()
        seq = [k for k in range(0, 10)] + [chr(k) for k in range(97, 103)] #[chr(k) for k in range(65, 91)] # + [chr(k) for k in range(97, 123)]
        if not self._collections:
            for i in seq:
                tablename = self._collection_name(str(i))
                collection = self._db.get_collection(tablename)
                indexes = collection.index_information()
                if not 'unid' in indexes:
                    collection.create_index('unid', unique=True, name='unid')
