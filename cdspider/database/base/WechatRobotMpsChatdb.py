#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""

from . import Base

{
    "wechat_robot_mps_chat": {
        "IUin": int,
        "MsgId" : str,
        "FromUserName" : str,
        "ToUserName" : str,
        "MsgType" : int,
        "Content" : str,
        "Type": str,
        "Text": str,
        "User": {
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
        },
    }
}

class WechatRobotMpsChatDB(Base):
    """
    wechat_robot_mps_chat data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError
