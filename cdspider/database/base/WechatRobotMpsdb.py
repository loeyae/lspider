#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""

from . import Base

{
    "wechat_robot_mps": {
        "IUin": int,
        "Uin" : int,
        "UserName" : str,
        "NickName" : str,
        "HeadImgUrl" : str,
        "ContactFlag" : int,
        "MemberCount" : int,
        "RemarkName" : str,
        "HideInputBarFlag" : int,
        "Sex" : int,
        "Signature" : str,
        "VerifyFlag" : int,
        "OwnerUin" : int,
        "PYInitial" : str,
        "PYQuanPin" : str,
        "RemarkPYInitial" : str,
        "RemarkPYQuanPin" : str,
        "StarFriend" : int,
        "AppAccountFlag" : int,
        "Statues" : int,
        "AttrStatus" : int,
        "Province" : str,
        "City" : str,
        "Alias" : str,
        "SnsFlag" : int,
        "UniFriend" : int,
        "DisplayName" : str,
        "ChatRoomId" : int,
        "KeyWord" : str,
        "EncryChatRoomId" : str,
        "IsOwner" : int
    }
}

class WechatRobotMpsDB(Base):
    """
    wechat_robot_mps data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError

    def delete_by_iuin(self, uin):
        raise NotImplementedError
