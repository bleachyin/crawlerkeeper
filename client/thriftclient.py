#!/usr/bin/python
# -*- coding: utf-8 -*- 

import Queue
import logging
import ujson
from functools import  wraps
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from crawlerkeeper.service.service import CrawlerThrift
from crawlerkeeper.common.jsonconst import THRIFT_STATUS_ERROR

class Client(object):
    
    DEFAULT_TIMEOUT = 5*1000 #5s
    
    def __init__(self,host,port,name):
        self._host = host
        self._port = port
        self._name = name
        self._logger = logging.getLogger(self.__class__.__name__)
        
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    @property
    def name(self):
        return self._name
    
    def ping(self):
        pass
    
    def logo(self,level,msg):
        pass
    
    def start(self):
        pass
    
    def stop(self):
        pass
    
    def pause(self):
        pass
    
    def resume(self):
        pass
    
    def get_running_info(self):
        pass

class CrawlerThriftClient(Client):
    
    def __init__(self,host,port,name):
        super(CrawlerThriftClient, self).__init__(host,int(port),name)
       
    def close(self):
        self._name = None
        if self._transport.isOpen():
            self._transport.close()
        if self._socket.isOpen():
            self._socket.close()
    
    def ping(self):
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            client.ping(self._name)
            transport.close()
        except Exception as ex:
            self._logger.error("CrawlerThriftClient [ping] occur exception due to : %s",ex)
        
    def logo(self,level,msg):
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            client.logo(self._name,level, msg)
            transport.close()
        except Exception as ex:
#             self._logger.error("CrawlerThriftClient [logo] occur exception due to : %s",ex)
            print "CrawlerThriftClient [logo] occur exception due to : %s" % str(ex)
#             pass

class CenterThriftClient(Client):
    
    def __init__(self, host, port, name):
        super(CenterThriftClient, self).__init__(host,port,name)
        
    def ping(self):
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            client.ping(self._name)
            transport.close()
        except Exception as ex:
            self._logger.error("CenterThriftClient [ping] occur exception due to : %s",ex) 
        
    def start(self):
        flag = THRIFT_STATUS_ERROR
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            flag = client.start(self._name)
            transport.close()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("CenterThriftClient [start] occur exception due to : %s",ex)
        finally:
            return flag
    
    def stop(self):
        flag = THRIFT_STATUS_ERROR
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            flag = client.stop(self._name)
            transport.close()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("CenterThriftClient [stop] occur exception due to : %s",ex)
        finally:
            return flag
    
    def pause(self):
        flag = THRIFT_STATUS_ERROR
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            client.pause(self._name)
            transport.close()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("CenterThriftClient [pause] occur exception due to : %s",ex)
        finally:
            return flag
    
    def resume(self):
        flag = THRIFT_STATUS_ERROR
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            flag = client.resume(self._name)
            transport.close()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("CenterThriftClient [resume] occur exception due to : %s",ex)
        finally:
            return flag
    
    def get_running_info(self):
        info = None
        try:
            socket = TSocket.TSocket(host=self._host, port=self._port)
            socket.setTimeout(self.DEFAULT_TIMEOUT)#5s
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
            client = CrawlerThrift.Client(iprot=protocol, oprot=protocol)
            transport.open()
            info = ujson.loads(client.get_running_info(self._name))
            transport.close()
        except Exception as ex:
            info = None
            self._logger.error("CenterThriftClient [get_running_info] occur exception due to : %s",ex)
        finally:
            return info
    
# thriftclient= ThriftClient()
# thriftclient.initialize("localhost",6888,"test")
# thriftclient.logo(20, "ccc")
