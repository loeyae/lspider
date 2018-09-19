#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""

from . import Base

{
    "wechat_robot_info": {
        "UserName" : str,
        "City" : str,
        "DisplayName" : str,
        "PYQuanPin" : str,
        "RemarkPYInitial" : str,
        "Province" : str,
        "KeyWord" : str,
        "RemarkName" : str,
        "PYInitial" : str,
        "EncryChatRoomId" : str,
        "Alias" : str,
        "Signature" : str,
        "NickName" : str,
        "RemarkPYQuanPin" : str,
        "HeadImgUrl" : str,
        "UniFriend" : int,
        "Sex" : int,
        "AppAccountFlag" : int,
        "VerifyFlag" : int,
        "ChatRoomId" : int,
        "HideInputBarFlag" : int,
        "AttrStatus" : int,
        "SnsFlag" : int,
        "MemberCount" : int,
        "OwnerUin" : int,
        "ContactFlag" : int,
        "Uin" : int,
        "StarFriend" : int,
        "Statues" : int,
        "WebWxPluginSwitch" : int,
        "HeadImgFlag" : int
    }
}

class WechatRobotInfoDB(Base):
    """
    wechat_robot_info data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError
