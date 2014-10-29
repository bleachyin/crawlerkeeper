#!/usr/bin/python
# -*- coding: utf-8 -*- 
#

import ujson
from crawlerkeeper.client.thriftclient import CenterThriftClient

# class RpcMetadata(object):
#     
#     def __init__(self, name, host, weight,port, max_fails = 10, fail_timeout = 15):
#         self.name = name
#         self.host = host
#         self.port = port
#         self.weight = weight
#         self.bucket = weight
#         self.fail_times = 0
#         self.max_fails = max_fails
#         self.fail_expired_time = 0
#         self.fail_timeout = fail_timeout
        

class RpcData(object):
    
    ZK_NODE_STATUS_PROCECSSED = 1
    ZK_NODE_STATUS_UNPROCESSED = 0

    def __init__(self,client_h,client_n,server_h="",server_n="",status=ZK_NODE_STATUS_UNPROCESSED,max_fails = 10, fail_timeout = 15):
        self._client_h = client_h #127.0.0.1:3678
        self._client_n = client_n
        self._server_h = server_h#127.0.0.1:3678
        self._server_n = server_n
        self._status = status
        self.fail_times = 0
        self.max_fails = max_fails
        self.fail_expired_time = 0
        self.fail_timeout = fail_timeout
        
    def dumps(self):
        return ujson.dumps({
                            "client_h":self._client_h,
                            "client_n":self._client_n,
                            "server_h":self._server_h,
                            "server_n":self._server_n,
                            "status":self._status,
                            },ensure_ascii=False)
    
    @property
    def client_h(self):
        return self._client_h
    
    @property
    def client_n(self):
        return self._client_n
    
    @property
    def server_h(self):
        return self._server_h
    
    @property
    def server_n(self):
        return self._server_n
    
    @property
    def status(self):
        return self._status
    
    def set_status(self,status):
        self._status = status
        
    def set_server_h(self,h):
        self._server_h = h

    def set_server_n(self,n):
        self._server_n = n
    
    @staticmethod
    def loads(data):
        info = ujson.loads(data)
        return RpcData(info['client_h'],info['client_n'],info['server_h'],info['server_n'],info['status'])
    
    def is_register(self):
        return self._status == self.ZK_NODE_STATUS_PROCECSSED


