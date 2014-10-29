#!/usr/bin/python
# -*- coding: utf-8 -*- 

#!/usr/bin/env python

import sys
import logging
import threading
import socket
from thrift.server.TServer import TThreadPoolServer
from thrift.transport.TSocket import TServerSocket
from crawlerkeeper.client.threadpoolprocessor import ThreadPoolProcessor
from crawlerkeeper.common.future import FutureThreadPool
from crawlerkeeper.notify.redismanager import RedisManager
sys.path.append('../service')

from crawlerkeeper.service.service import CrawlerThrift
from crawlerkeeper.service.service.ttypes import *

from threading import Thread
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from crawlerkeeper.client.threadpool import ThreadPool


class BaseThriftServerHandler(object):
    
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        
    def ping(self,servicename):
        pass
    
    def logo(self,servicename,level,msg):
        pass
    
    def start(self,servicename):
        pass
    
    def stop(self,servicename):
        pass
    
    def pause(self,servicename):
        pass
    
    def resume(self,servicename):
        pass
    
    def get_running_info(self,servicename):
        pass
    
class CrawlerThriftHandler(BaseThriftServerHandler):
    
    def __init__(self):
        super(CrawlerThriftHandler, self).__init__()
        
    def start(self,servicename):#for client-crawler
        self._logger.warn("thriftserver recieve  [start] signal servicename is %s",servicename)
        return ThreadPoolProcessor.processor_future_start()
        
#         if not self._thread_pool_processor:
#             self._logger.error("client-crawler [start] error due to threadpoolprocessor is none")
#         else:
#             try:
#                 self._thread_pool_processor.processor_start()
#             except Exception as ex:
#                 self._logger.error("client-crawler [start] error due to : %s",ex)

    def stop(self,servicename):#for client-crawler 
        self._logger.warn("thriftserver recieve  [stop] signal servicename is %s",servicename)
        return ThreadPoolProcessor.processor_stop()
    
    def pause(self,servicename):#for client-crawler
        self._logger.warn("thriftserver recieve  [pause] signal servicename is %s",servicename)
        return ThreadPoolProcessor.processor_future_pause()
#         return 1

    def resume(self, servicename):#for client-crawler
        self._logger.warn("thriftserver recieve  [resume] signal servicename is %s",servicename)
        return ThreadPoolProcessor.processor_resume()
    
    def get_running_info(self,servicename):
        self._logger.warn("thriftserver recieve  [get_running_info] signal servicename is %s",servicename)
        return ThreadPoolProcessor.processor_running_info()

class CenterThriftHandler(BaseThriftServerHandler):
    
    def __init__(self):
        super(CenterThriftHandler, self).__init__()

    def ping(self,servicename):
        return 1

    def logo(self,servicename,level,msg):
        self._logger.debug("servicename:%s level:%s msg:%s",servicename,level,msg)
        RedisManager.instance().set_logo(servicename,level,msg)

class TThreadPoolServerWithStop(TThreadPoolServer):
    
    def __init__(self, *args, **kwargs):
        self._stop = False
        TServer.TThreadPoolServer.__init__(self, *args)
    
    def serve(self):
        """Start a fixed number of worker threads and put client into a queue"""
        for i in range(self.threads):
            try:
                t = threading.Thread(target = self.serveThread)
                t.setDaemon(self.daemon)
                t.start()
            except Exception, x:
                logging.exception(x)
        # Pump the socket for clients
        self.serverTransport.listen()
        while not self._stop:
            try:
                client = self.serverTransport.accept()
                self.clients.put(client)
            except Exception, x:
                logging.exception(x)
    
    def stop(self):
        self._stop = True
        self.serverTransport.close()

STATUS_SERVER_RUN = 1
STATUS_SERVER_STOP = 0

class Server(Thread):
    
    def __init__(self,host,port):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._host = host
        self._port = int(port)
        self._status = STATUS_SERVER_STOP
#         self._thread_pool_processor = None
    
#     def initialize(self, thrift_host, thrift_port):
#         self._thrift_host = thrift_host
#         self._thrift_port = thrift_port
#         handler = CrawlerThriftHandler(self._thread_pool_processor)
#         self._processor = CrawlerThrift.Processor(handler)
#         self._transport = TSocket.TServerSocket(self._thrift_host,self._thrift_port)
#         self._tfactory = TTransport.TBufferedTransportFactory()
#         self._pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        
#     def add_thread_pool_processor(self,thread_pool_processor):
#         self._thread_pool_processor = thread_pool_processor
    
    def is_same_conn(self,host,port):
        return ":".join((host,str(port))) == ":".join((self._host,str(self._port)))
    
    def run(self):
        self._serve()

    def _serve(self):
        try:
            self._processor = CrawlerThrift.Processor(self._handler)
            self._transport = TSocket.TServerSocket(self._host,self._port)
            self._tfactory = TTransport.TBufferedTransportFactory()
            self._pfactory = TBinaryProtocol.TBinaryProtocolFactory()
#             self._server = TThreadPoolServerWithStop(self._processor, self._transport, self._tfactory, self._pfactory)
            self._server = TThreadPoolServerWithStop(self._processor, self._transport, self._tfactory, self._pfactory)
            self._status = STATUS_SERVER_RUN
            self._server.serve()
        except Exception as ex:
            self._logger.error("thrift server start occur exception due to: %s", ex)
            self._status = STATUS_SERVER_STOP 
            
    def stop(self):
        try:
            if self._server:
                self._server.stop()
                self._status = STATUS_SERVER_STOP
        except Exception as ex:
            self._logger.error("thrift server stop occur exception due to: %s ", ex)

    def is_running(self):
        return self._status == STATUS_SERVER_RUN
    
class CrawlerClientServer(Server):
    
    def __init__(self,host,port):
        self._handler = CrawlerThriftHandler()
        super(CrawlerClientServer, self).__init__(host,port)
        
class CenterClientServer(Server):
    
    def __init__(self,host,port):
        self._handler = CenterThriftHandler()
        super(CenterClientServer, self).__init__(host,port)
    
# print "Starting python server..."
# s = Server()
# s.initialize("localhost", 6999)
# s._serve()
# print "done!"