#!/usr/bin/python
# -*- coding: utf-8 -*- 

class RedisManager(object):

    def __init__(self, _redis_pool):
        self._redis = _redis_pool.get_redis()

    @classmethod
    def instance(cls):
        return cls._instance
        
    @classmethod
    def initialize(cls, redis_pool):
        cls._instance = cls(redis_pool)

    def set_logo(self,servicename,level,msg):
        key = ":".join((servicename,str(level)))
        num = self._redis.rpush(key,msg)
        if num > 1000:
            self._redis.lpop(key)
        
    def get_logo(self,servicename,level,start=0,end=300):
        key = ":".join((servicename,str(level)))
        return self._redis.lrange(key,-end,-start-1)
    
#     def redis_pub(self, channel, pushstr):
#         self._redis.publish(channel, pushstr)

