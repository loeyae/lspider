#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-29 20:23:59
"""
import os
import json
import time
import tempfile
import traceback
import logging
import itchat
from six.moves import queue
from itchat.content import *
from cdspider import Component
from cdspider.libs import utils

class WxchatRobots(Component):
    """
    WEB微信机器人
    """
    def __init__(self, db, queue, uuid, data_dir = None, debug = False, log_level = logging.WARN):
        self.db = db
        self.queue = queue
        self.message_queue = None
        if queue and 'wechat2reply' in queue:
            self.message_queue = queue['wechat2reply']
        self.debug_mode = debug
        self.log_level = log_level
        self.qrfile = None
        logger = logging.getLogger('robots')
        super(WxchatRobots, self).__init__(logger, log_level)
        self.__uid = uuid
        self.temp_dir = data_dir or tempfile.gettempdir()
        self.auto_reply = None

    def get_message(self, message, uuin):
        s = self.auto_reply(message, uuin)
        data = json.loads(s)
        if data['status'] == 200:
            return data['data']
        return "忙碌中..."

    def set_reply(self, fun):
        self.auto_reply = fun

    def run(self):

        statusStorageDir = 'wechat%s.pkl' % self.__uid
        itchat.set_logging(showOnCmd=self.debug_mode, loggingLevel = self.log_level)
        robot = itchat.new_instance()

        def login():
            self.info("The Wechat was login @ Process of No.%s " % (self.__uid))
            if self.qrfile and os.path.isfile(self.qrfile):
                os.remove(self.qrfile)
                self.qrfile = None
            #获取联系人，并保存
            if self.db:
                db = self.db['base']
                myself = robot.search_friends()
                if myself:
                    try:
                        db.insert(myself, table = "wechat_robot_info")
                    except:
                        pass
                for item in robot.get_contact():
                    try:
                        db.insert(item, table = "wechat_robot_contact")
                    except:
                        pass
                for item in robot.get_friends():
                    try:
                        db.insert(item, table = "wechat_robot_friends")
                    except:
                        pass
                for item in robot.get_chatrooms():
                    try:
                        db.insert(item, table = "wechat_robot_chatrooms")
                    except:
                        pass
                for item in robot.get_mps():
                    try:
                        db.insert(item, table = "wechat_robot_mps")
                    except:
                        pass

        def logout():
            self.info("The Wechat was logout @ Process of No.%s " % (self.__uid))

        def qr_callback(**kwargs):
            try:
                qrcode = kwargs.get('qrcode')
                if qrcode:
                    dirname = os.path.realpath(os.path.join(self.temp_dir, "qr", "service"))
                    qrfile = "wxqr_%s.png" % (self.__uid)
                    if os.path.exists(dirname) == False:
                        os.makedirs(dirname)
                    self.qrfile = os.path.join(dirname, qrfile)
                    self.info(self.qrfile)
                    with open(self.qrfile, 'wb') as f:
                        f.write(qrcode)
            except:
                self.error(traceback.format_exc())

        @robot.msg_register([TEXT, MAP, CARD, NOTE, SHARING])
        def text_reply(msg):
            if self.db:
                self.db['base'].insert(msg, table = "wechat_robot_chat")
#            reply = self.get_message(msg.text, msg.user.userName)
#            msg.user.send('%s' % reply)
            if self.message_queue:
                self.message_queue.put_nowait({"user": msg.user.userName, "msg": msg.text, "nick": "", "auser": ""})

        @robot.msg_register([TEXT, MAP, CARD, NOTE, PICTURE, RECORDING, VOICE, ATTACHMENT, VIDEO], isMpChat=True)
        def text_replay(msg):
            if self.db:
                try:
                    self.db['base'].insert(msg, table = "wechat_robot_mp_chat")
                except:
                    pass

        @robot.msg_register(SHARING, isMpChat=True)
        def text_replay(msg):
            if self.db:
                try:
                    self.db['base'].insert(msg, table = "wechat_robot_mp_sharing")
                except:
                    pass

        @robot.msg_register([PICTURE, RECORDING, ATTACHMENT, VIDEO])
        def download_files(msg):
            msg.download(msg.fileName)
            typeSymbol = {
                PICTURE: 'img',
                VIDEO: 'vid', }.get(msg.type, 'fil')
            self.info('@%s@%s' % (typeSymbol, msg.fileName))
            return '@%s@%s' % (typeSymbol, msg.fileName)

        @robot.msg_register(FRIENDS)
        def add_friend(msg):
            msg.user.verify()
            if self.db:
                self.db['base'].insert(msg, table = "wechat_robot_new_friend")

        @robot.msg_register(TEXT, isGroupChat=True)
        def text_reply(msg):
            if self.db:
                self.db['base'].insert(msg, table = "wechat_robot_group_chat")
            if msg.isAt:
                if self.message_queue:
                    self.message_queue.put_nowait({"user": msg.user.userName, "msg": msg.text.split(u'\u2005')[1], "nick": msg.actualNickName, "auser": msg.actualUserName})
#                reply = self.get_message(msg.text, msg.user.userName)
#                msg.user.send(u'@%s\u2005 %s' % (
#                    msg.actualNickName, reply))

        try:
            self.info("wechat will running")
            f = None if self.debug_mode else qr_callback
            robot.auto_login(hotReload=True, statusStorageDir=statusStorageDir, loginCallback=login, exitCallback=logout, qrCallback=f, enableCmdQR=self.debug_mode)
            #自动回复
            t = utils.run_in_thread(self.reply_fn, robot)
            robot.run(self.debug_mode)
        except:
            self.error(traceback.format_exc())

    def reply_fn(self, robot):
        if not self.message_queue:
            return
        self.info("auto reply is starting")
        while True:
            try:
                message = self.message_queue.get_nowait()
                uuid = "%s%s" % (message['user'], message['auser'])
                nick = message['nick']
                reply = self.get_message(message['msg'], uuid)
                if reply:
                    if nick:
                        reply = u'@%s\u2005%s' % (nick, reply)
                    robot.send(reply, message['user'])
                time.sleep(0.1)
            except queue.Empty:
#                self.debug("queue is empty")
                time.sleep(0.1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.error(traceback.format_exc())
