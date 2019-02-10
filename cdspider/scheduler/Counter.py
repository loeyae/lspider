#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019-1-30 20:32:35
"""
from __future__  import division
import os
import pickle
import logging
import six
import math
import time

class Counter(object):
    """
    counter
    """
    def __init__(self, **kwargs):
        self.data = {"offset": 0}
        for k, v in kwargs.items():
            self.data[k] = v

    @property
    def stime(self):
        '''
        开始时间
        '''
        return self.data.get('stime')

    @stime.setter
    def stime(self, value):
        self.data['stime'] = value

    @property
    def ctime(self):
        '''
        持续时间
        '''
        return self.data.get('ctime')

    @ctime.setter
    def ctime(self, value):
        self.data['ctime'] = value

    @property
    def itime(self):
        '''
        间隔时间
        '''
        return self.data.get('itime')

    @itime.setter
    def itime(self, value):
        self.data['itime'] = value

    @property
    def total(self):
        '''
        总任务数
        '''
        return self.data.get('total')

    @total.setter
    def total(self, value):
        self.data['total'] = value

    @property
    def ltime(self):
        '''
        上一次入队时间
        '''
        return self.data.get('ltime', 0)

    @ltime.setter
    def ltime(self, value):
        self.data['ltime'] = value

    @property
    def now(self):
        '''
        上一次入队时间
        '''
        return self.data.get('now')

    @now.setter
    def now(self, value):
        self.data['now'] = value

    @property
    def avg(self):
        """
        平均每次需要入队的任务数
        """
        if 'avg' in self.data:
            return self.data['avg']
        else:
            if self.total and self.ctime and self.itime:
                self.data['avg'] = math.ceil(self.total / (self.ctime / self.itime))
                return self.data['avg']
        return None

    @property
    def count(self):
        c = 0
        if self.ltime:
            itime = self.now - self.ltime
            p = round(itime / self.itime)
            c = self.avg * p
        else:
            c = self.avg
        return c

    @property
    def offset(self):
        if 'offset' in self.data:
            return self.data['offset']
        return 0

    @offset.setter
    def offset(self, value):
        self.data['offset'] = value

    def empty(self):
        self.data = []

class CounterMananger(object):

    def __init__(self, cpath, key, cls=Counter):
        if isinstance(key, six.string_types):
            key = (key, 0)
        data_dir = os.path.join(cpath, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        self.filename = os.path.join(data_dir, 'counter_%s_%s' % key)
        self.cls = cls
        if not self.load():
            self.counter = cls()
        self.counter.now = int(time.time())

    def __del__(self):
        self.dump()

    def event(self, **kwargs):
        for key,val in kwargs.items():
            if not getattr(self.counter, key):
                setattr(self.counter, key, val)
        return self

    def get(self, key):
        return getattr(self.counter, key)

    def value(self, value):
        if value > 0:
            self.counter.ltime = self.counter.now
            self.counter.offset += value

    def counter(self):
        return self.counter

    def dump(self):
        try:
            with open(self.filename, 'wb') as fp:
                pickle.dump(self.counter, fp)
        except Exception as e:
            logging.error("can't dump counter to file %s: %s", self.filename, e)
            return False
        return True

    def load(self):
        try:
            with open(self.filename, 'rb') as fp:
                self.counter = pickle.load(fp, encoding='latin1')
        except Exception as e:
            logging.error("can't load counter from file %s: %s", self.filename, e)
            return False
        return True
