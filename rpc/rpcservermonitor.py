#!/usr/bin/python
# -*- coding: utf-8 -*- 
#

import logging
import time
import threading
import Queue
from threading import Thread

from crawlerkeeper.rpc.zkclient import ZKClient
from crawlerkeeper.rpc.rpcmetadata import RpcData

class _RpcServerMonitor(Thread):#for server

    # ZK_MONITOR_THREAD_NAME = "ZkMonitor"
    
    def __init__(self):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._zkQueue = Queue.Queue()
        self._settings = None

    def initialize(self, zk_server,zk_path,server_host,server_name):
        self._servers = zk_server
        self._zkpaths = zk_path
        self._server_host= server_host
        self._server_name = server_name
#         self._servers = self._settings['ZOOKEEPER_SERVER']
#         self._zkpaths = self._settings['ZOOKEEPER_PATH']
        self._client = ZKClient(servers=self._servers)
        self._build_rpc_nodes(self._zkpaths)
        self.start()
                        
    def stop(self):
        if self._client is not None:
            self._client.close()
            self._client = None
        self._logger.info("Zookeeper monitor stopped.")
        
    def run(self):
        self._logger.info("Zookeeper monitor start to work...")
        while 1:
            try:
                path = self._zkQueue.get()
                self._logger.info("Zookeeper path %s changed", path)
                self._reconnect()
                self._build_rpc_nodes(self._zkpaths)
            except Exception as ex:
                self._logger.error("Get message from zk queue, occur exception:%s", str(ex), exc_info=1)
                pass
        
    def _reconnect(self):
        while 1:
            try:
                self._client.close()
                self._client = ZKClient(servers=self._servers)
                return
            except Exception as ex:
                time.sleep(3) # wait 3 seconds and try again
                self._logger.fatal("Connect to zookeeper, occur exception: %s", str(ex), exc_info=1)
                
    def add_register_callback(self,callback):
        self._callback = callback
                    
    def _rpc_zk_watcher(self, h, type, state, path):
        self._logger.info("child changed: %s at %r state: %s", type, state, path)
        self._zkQueue.put(path) # notify zookeeper monitor to refresh service nodes           

    def _build_rpc_nodes(self, path):
        try:
            children = self._client.get_children(path, self._rpc_zk_watcher)
            self._logger.info("Zookeeper get children:%s from path:%s", path, children)
            changedNodes = {}
            for child in children:
                fullpath = '/'.join((path, child))
                try:                
                    data, stat = self._client.get(fullpath)
                    self._logger.info("Get rpc node[%s] raw-data[%s], stat[%s]", fullpath, data, stat)
                    rpcdata = RpcData.loads(data)
                    if not rpcdata.is_register(): #skip set secret
                        rpcdata.set_status(RpcData.ZK_NODE_STATUS_PROCECSSED)
                        rpcdata.set_server_h(self._server_host)
                        rpcdata.set_server_n(self._server_name)
#                         rpcdata.set_server_h(self._settings['THRIFT_SERVER'])
#                         rpcdata.set_server_n(self._settings['THRIFT_SERVER_NAME'])
                        self._client.set(path=fullpath, data=rpcdata.dumps())
                    changedNodes[child] = rpcdata
                    self._logger.info("Get rpc node[%s] info[%s:%s]", fullpath, rpcdata.client_h, rpcdata.client_n)
                except Exception as ex:
                    self._logger.error("Get data from zookeeper[%s] occur exception", fullpath, exc_info=1)
                    continue
            if self._callback:
                self._callback(changedNodes)
            else:
                raise AttributeError("rpcservermonitor does not add callback before init")
        except Exception as ex:
            self._logger.fatal("Get children from zookeeper[%s] occur exception", path, exc_info=1)

RpcServerMonitor = _RpcServerMonitor()