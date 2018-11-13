#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-11 10:58:26
"""
BLOOMFILTER_HASH_NUMBER = 6
BLOOMFILTER_BIT = 30

class HashMap(object):
    def __init__(self, m, seed):
        self.m = m
        self.seed = seed

    def hash(self, value):
        """
        Hash Algorithm
        :param value: Value
        :return: Hash Value
        """
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.m - 1) & ret


class BloomFilter(object):
    """
    put you comment
    """
    def __init__(self, server, key, bit=BLOOMFILTER_BIT, hash_number=BLOOMFILTER_HASH_NUMBER):
        """
        Initialize BloomFilter
        :param server: Redis Server
        :param key: BloomFilter Key
        :param bit: m = 2 ^ bit
        :param hash_number: the number of hash function
        """
        # default to 1 << 30 = 10,7374,1824 = 2^30 = 128MB, max filter 2^30/hash_number = 1,7895,6970 fingerprints
        self.m = 1 << bit
        self.seeds = range(hash_number)
        self.server = server
        self.key = key
        self.maps = [HashMap(self.m, seed) for seed in self.seeds]

    def exists(self, value):
        """
        if value exists
        :param value:
        :return:
        """
        if not value:
            return False
        exist = True
        for m in self.maps:
            offset = m.hash(value)
            exist = exist & self.server.getbit(self.key, offset)
        return exist

    def insert(self, value):
        """
        add value to bloom
        :param value:
        :return:
        """
        for f in self.maps:
            offset = f.hash(value)
            self.server.setbit(self.key, offset, 1)

    def seen(self, value):
        if self.exists(value):
            return False
        self.insert(value)
        return True
