#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""
import time
import pymongo
from cdspider.database.base import WechatRobotInfoDB as BaseWechatRobotInfoDB
from .Mongo import Mongo

class WechatRobotInfoDB(Mongo, BaseWechatRobotInfoDB):
    """
    wechat_robot_info data object
    """
    __tablename__ = 'wechat_robot_info'

    def __init__(self, connector, table=None, **kwargs):
        super(WechatRobotInfoDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'Uin' in indexes:
            collection.create_index('Uin', unique=True, name='Uin')
        if not 'UserName' in indexes:
            collection.create_index('UserName', name='UserName')
        if not 'NickName' in indexes:
            collection.create_index([('NickName', pymongo.TEXT)], name='NickName')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj = {}):
        uin = obj.pop('Uin')
        where = {"Uin": uin}
        obj.setdefault('ctime', int(time.time()))
        super(WechatRobotInfoDB, self).update(setting=obj, where=where, upsert=True)
        return uin

    def delete(self, id):
        where = {"Uin": uin}
        super(WechatRobotChatRoomsDB, self).delete(where=where, multi=False)
