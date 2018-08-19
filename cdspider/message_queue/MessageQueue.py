#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:56:34
"""

import time
import json
import socket
import select
import logging
import umsgpack
import threading
import pika
import pika.exceptions
import amqp
import errno
from six.moves.urllib.parse import unquote
from six.moves import queue as BaseQueue
from cdspider.message_queue import BaseQueue as CDBaseQueue
connection_pool = {}
logger = logging.getLogger('queue')

def catch_error(func):
    """Catch errors of rabbitmq then reconnect"""
    import amqp
    try:
        import pika.exceptions
        connect_exceptions = (
            pika.exceptions.ConnectionClosed,
            pika.exceptions.AMQPConnectionError,
        )
    except ImportError:
        connect_exceptions = ()

    connect_exceptions += (
        select.error,
        socket.error,
        amqp.ConnectionError,
        amqp.exceptions.RecoverableConnectionError,
    )

    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except connect_exceptions as e:
            logger.error('RabbitMQ error: %r, reconnect.', e)
            self.connect()
            return func(self, *args, **kwargs)
    return wrap

class PikaQueue(CDBaseQueue):
    """
    A Queue like rabbitmq connector
    """

    Empty = BaseQueue.Empty
    Full = BaseQueue.Full
    max_timeout = 0.3

    def __init__(self, name, user="guest",exchange='', password="guest", host="localhost", port=5672, path='%2F',
                 maxsize=0, lazy_limit=True, log_level = logging.WARN):
        """
        init
        """
        super(PikaQueue, self).__init__(name=name, exchange=exchange, user=user, password=password, host=host, port=port, path=path,
                 maxsize=maxsize, lazy_limit=lazy_limit, log_level=log_level)
        self.lock = threading.RLock()
        if self.lazy_limit and self.maxsize:
            self.qsize_diff_limit = int(self.maxsize * 0.1)
        else:
            self.qsize_diff_limit = 0
        self.qsize_diff = self.qsize() - self.maxsize

    def connect(self):
        """
        连接queue服务器
        """
        k = self.symbol()
        if not k in connection_pool:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(self.host,
                                           self.port,
                                           unquote(self.path.lstrip('/') or '%2F'),
                                           credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            connection_pool[k] = self.connection
        else:
            self.connection = connection_pool[k]
            self.channel = self.connection.channel()
        try:
            self.channel.queue_declare(self.queuename)
        except pika.exceptions.ChannelClosed:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            connection_pool[k] = self.connection
            self.channel.queue_declare(self.queuename)
        #self.channel.queue_purge(self.queuename)

    @catch_error
    def qsize(self):
        with self.lock:
            ret = self.channel.queue_declare(self.queuename, passive=True)
        return ret.method.message_count

    def empty(self):
        if self.qsize() == 0:
            return True
        else:
            return False

    def full(self):
        if self.maxsize and self.qsize() >= self.maxsize:
            return True
        else:
            return False

    @catch_error
    def put(self, obj, block=True, timeout=None):
        if not block:
            return self.put_nowait()

        start_time = time.time()
        while True:
            try:
                return self.put_nowait(obj)
            except BaseQueue.Full:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    @catch_error
    def put_nowait(self, obj, pack = True):
        if self.lazy_limit and self.qsize_diff < self.qsize_diff_limit:
            pass
        elif self.full():
            raise BaseQueue.Full
        else:
            self.qsize_diff = 0
        with self.lock:
            self.qsize_diff += 1
            if pack:
                return self.channel.basic_publish("", self.queuename, umsgpack.packb(obj))
            return self.channel.basic_publish("", self.queuename, json.dumps(obj))

    @catch_error
    def get(self, block=True, timeout=None, ack=False):
        if not block:
            return self.get_nowait()

        start_time = time.time()
        while True:
            try:
                return self.get_nowait(ack)
            except BaseQueue.Empty:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    @catch_error
    def get_nowait(self, ack=False):
        with self.lock:
            method_frame, header_frame, body = self.channel.basic_get(self.queuename, not ack)
            if method_frame is None:
                raise BaseQueue.Empty
            if ack:
                self.channel.basic_ack(method_frame.delivery_tag)
        try:
            s=umsgpack.unpackb(body)
        except:
            s=json.loads(body.decode())
        return s

    @catch_error
    def delete(self):
        with self.lock:
            return self.channel.queue_delete(queue=self.queuename)

    def close(self):
        pass

class AmqpQueue(PikaQueue):

    def __init__(self, name, user="guest",exchange='', password="guest", host="localhost", port=5672, path='%2F',
                 maxsize=0, lazy_limit=True, log_level=logging.WARN):
        """
        init
        """
        super(AmqpQueue, self).__init__(name=name, exchange=exchange,user=user, password=password, host=host, port=port, path=path,
                 maxsize=maxsize, lazy_limit=lazy_limit, log_level=log_level)

    def connect(self):
        """
        连接rabbitmq服务器
        """
        k = self.symbol()
        if not k in connection_pool:
            self.connection = amqp.Connection(host="%s:%s" % (self.host, self.port),
                                              userid=self.username or 'guest',
                                              password=self.password or 'guest',
                                              virtual_host=unquote(
                                                  self.path.lstrip('/') or '%2F'))
            self.connection.connect()
            self.channel = self.connection.channel()
            connection_pool[k] = self.connection
        else:
            self.connection = connection_pool[k]
            if not self.connection.connected:
                del connection_pool[k]
                self.connect()
            else:
                self.channel = self.connection.channel()
        try:
            self.channel.queue_declare(self.queuename, durable=True)
        except amqp.exceptions.PreconditionFailed:
            pass

    @catch_error
    def qsize(self):
        with self.lock:
            name, message_count, consumer_count = self.channel.queue_declare(
                self.queuename, durable=True, passive=True)
        return message_count

    @catch_error
    def put_nowait(self, obj, pack = True):
        if self.lazy_limit and self.qsize_diff < self.qsize_diff_limit:
            pass
        elif self.full():
            raise BaseQueue.Full
        else:
            self.qsize_diff = 0
        with self.lock:
            self.qsize_diff += 1
            if pack:
                msg = amqp.Message(umsgpack.packb(obj))
            else:
                msg = amqp.Message(json.dumps(obj))
            return self.channel.basic_publish(msg, exchange=self.exchange, routing_key=self.queuename)

    @catch_error
    def get_nowait(self, ack=False):
        with self.lock:
            message = self.channel.basic_get(self.queuename, not ack)
            if message is None:
                raise BaseQueue.Empty
            if ack:
                self.channel.basic_ack(message.delivery_tag)
        try:
            s=umsgpack.unpackb(message.body)
        except:
            s=json.loads(message.body.decode())
        return s
