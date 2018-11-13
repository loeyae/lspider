#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""

from . import Base

{
    "wechat_robot_group_chat": {
        "IUin": int,
        "MsgId" : str,
        "FromUserName" : str,
        "ToUserName" : str,
        "MsgType" : int,
        "Content" : str,
        "Status" : int,
        "ImgStatus" : int,
        "CreateTime" : int,
        "VoiceLength" : int,
        "PlayLength" : int,
        "FileName" : str,
        "FileSize" : str,
        "MediaId" : str,
        "Url" : str,
        "AppMsgType" : int,
        "StatusNotifyCode" : int,
        "StatusNotifyUserName" : str,
        "HasProductId" : 0,
        "Ticket" : "",
        "ImgHeight" : int,
        "ImgWidth" : int,
        "SubMsgType" : int,
        "NewMsgId" : int,
        "OriContent" : str,
        "EncryFileName" : str,
        "ActualNickName" : str,
        "IsAt" : bool,
        "ActualUserName" : str,
        "User" : {
            "MemberList" : [
                {
                    "Uin" : int,
                    "UserName" : str,
                    "NickName" : str,
                    "AttrStatus" : int,
                    "PYInitial" : str,
                    "PYQuanPin" : str,
                    "RemarkPYInitial" : str,
                    "RemarkPYQuanPin" : str,
                    "MemberStatus" : int,
                    "DisplayName" : str,
                    "KeyWord" : str
                }
            ],
            "UserName" : str,
            "NickName" : str,
            "Sex" : int,
            "HeadImgUpdateFlag" : int,
            "ContactType" : int,
            "Alias" : str,
            "ChatRoomOwner" : str,
            "HeadImgUrl" : str,
            "ContactFlag" : int,
            "MemberCount" : int,
            "HideInputBarFlag" : int,
            "Signature" : str,
            "VerifyFlag" : int,
            "RemarkName" : str,
            "Statues" : int,
            "AttrStatus" : int,
            "Province" : str,
            "City" : str,
            "SnsFlag" : int,
            "KeyWord" : str,
            "OwnerUin" : int,
            "IsAdmin" : bool
        },
        "Type" : str,
        "Text" : str
    }
}

class WechatRobotGroupChatDB(Base):
    """
    wechat_robot_group_chat data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError
