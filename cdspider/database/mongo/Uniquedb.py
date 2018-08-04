#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:36:43
:version: SVN: $Id: Uniquedb.py 2430 2018-07-31 01:30:48Z zhangyi $
"""
from pymongo.errors import *
from cdspider.database.base import UniqueDB as BaseUniqueDB
from .Mongo import Mongo, SplitTableMixin

class UniqueDB(Mongo, BaseUniqueDB, SplitTableMixin):

    __tablename__ = 'unique'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(UniqueDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj, projectid, taskid, urlid, attachid, kwid, createtime):
        unid = self.build(obj)
        table = self._table_name(unid)
        try:
            _id = super(UniqueDB, self).insert({"unid": unid, "projectid": projectid, "taskid": taskid, "urlid": urlid, "attachid": attachid, "kwid": kwid, "createtime": createtime}, table=table)
            if _id:
                return (True, {'unid': unid, 'createtime': createtime})
            else:
                return (False, False)
        except DuplicateKeyError:
            result = self.get(where={"unid": unid}, table=table)
            return (False, {"unid": result['unid'], "createtime": result['createtime']})
        except:
            return (False, False)

    def _table_name(self, unid):
        return super(UniqueDB, self)._collection_name(unid[0:1])

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
                    collection.create_index('projectid', name='projectid')
                    collection.create_index('siteid', name='siteid')
                    collection.create_index('urlid', name='urlid')
                    collection.create_index('attachid', name='attachid')
                    collection.create_index('kwid', name='kwid')
