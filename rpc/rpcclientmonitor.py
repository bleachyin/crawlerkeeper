# coding: utf-8

import logging
import Queue
import threading
from threading import Thread
from crawlerkeeper.rpc.zkclient import ZKClient
from crawlerkeeper.rpc.rpcmetadata import  RpcData

class _RpcClientMonitor(Thread):
    
    RPC_CLIENT_THREAD_NAME = "rpc_client"
    
    def __init__(self):
        threading.Thread.__init__(self, name=self.RPC_CLIENT_THREAD_NAME)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._rpcCallbackQueue = Queue.Queue()
    
    def initialize(self, servers, zkpaths):
        self._servers = servers
        self._zkpaths = zkpaths
        self._client = ZKClient(servers=self._servers)
        self._callback = None
        try:
            if zkpaths and not self._client.exists(zkpaths):
                self._client.create_persistence_node(zkpaths,'')
        except Exception as ex:
           	self._logger.error("rpcclient initialize occur exception due to : %s",ex)
#         for k in self._zkpaths.iterkeys():
#             self._build_rpc_nodes(k)
        self.start()
        
    def add_register_callback(self,rpc_node,callback):
        self._callback = callback
        self._c_path = self._zkpaths
        self._c_rpc_node = rpc_node
        self._c_full_path = "/".join((self._c_path,rpc_node.client_n))
        self._rpcCallbackQueue.put(self._c_path) 
    
    
    def _rpc_get_watcher(self, h, type, state, path):
        self._logger.info("child changed: %s at %r state: %s", type, state, path)
        self._rpcCallbackQueue.put(path) # notify zookeeper monitor to refresh service nodes

#     _rpc_get_wathcer = _rpc_exists_watcher

    def _callback_register_event(self):
        if self._client.exists(self._c_full_path,self._rpc_get_watcher):
            data,stat = self._client.get(self._c_full_path,)
            rpcdata = RpcData.loads(data)
            if rpcdata.is_register():
                self._callback(rpcdata) #return to the callback method
        else:
            self.do_register(self._c_path,self._c_rpc_node)
#             except Exception as ex:
#                 self._logger.error("listen node occur exception due to:%s",ex)

    def run(self):
        while 1:
            try:
                path = self._rpcCallbackQueue.get()
                self._callback_register_event()
            except Exception as ex:
                self._logger.error("while run callback register event occur exception due to:%s",ex)
        
    def _create_node(self,path,rpc_node):
        if rpc_node:
            path = "/".join((path,rpc_node._client_n))
            data = rpc_node.dumps()
            try:
                if self._client.exists(path):
                    self._client.set(path, data)
                    self._logger.info("the node %s is exists overwrite the node data", path)
                else:
                    self._client.create(path, data)
                    self._logger.info("create node %s with data %s",path,data)
            except Exception as ex:
                self._logger.error("self create node occur exception due to : %s",ex)
            
    def do_register(self,path,rpc_node):
        self._logger.info("Register path[%s] info[%s]",path,rpc_node.client_h)
        self._create_node(path, rpc_node)

    def close(self):
        pass

RpcClientMonitor = _RpcClientMonitor()

# client = RpcClient()
# client.initialize(ZK_CONF['server'], ZK_CONF['path'])
# client.add_register_callback("/mitv/crawler", RpcData("localhost","5777","crawlertest",RpcData.ZK_NODE_STATUS_PROCECSSED), node_print)

# while 1:
#     rpcclient = rpcClient()
#     rpcclient.initialize(ZK_CONF["server"],ZK_CONF["path"])
#     rpc_node = RpcMetadata("crawler1", "172.27.22.38", 1,9306 ,max_fails = 10, fail_timeout = 15)
#     rpcclient.register(ZK_CONF["path"], rpc_node)
#     time.sleep(10)