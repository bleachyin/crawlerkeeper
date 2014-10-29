#!/usr/bin/python
# -*- coding: utf-8 -*- 

import redis

class RedisPool(object):

#     @classmethod
#     def instance(cls):
#         if not hasattr(cls, '_instance'):
#             cls._instance = cls()
#         return cls._instance

    def __init__(self,host,port,db):
        self._redis_pool = redis.ConnectionPool(host= host,
                                                port= port,
                                                db=db)

    def get_redis(self):
        return redis.Redis(connection_pool=self._redis_pool)

    def close(self):
        if self._redis_pool:
            self._redis_pool.disconnect()
            self._redis_pool = None
            
