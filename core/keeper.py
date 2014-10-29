#!/usr/bin/python
# coding: utf-8
#
# Copyright 2011 DuoKan
#
import logging
from crawlerkeeper.client.threadpoolprocessor import ThreadPoolProcessor
from crawlerkeeper.rpc.rpcclientmonitor import RpcClientMonitor
from crawlerkeeper.rpc.rpcmetadata import RpcData
from crawlerkeeper.server.thriftserver import CrawlerClientServer, \
    CenterClientServer
from crawlerkeeper.client.thriftclient import CrawlerThriftClient, \
    CenterThriftClient
from crawlerkeeper.common.future import FutureThreadPool
from crawlerkeeper.rpc.rpcservermonitor import RpcServerMonitor
from crawlerkeeper.notify.redispool import RedisPool
from crawlerkeeper.notify.redismanager import RedisManager
from flop.lock.rwlock import ReadWriteLock


class Keeper(object):
    
    def __init__(self):
        self._rpcRwLock = ReadWriteLock()
        self._logger = logging.getLogger(self.__class__.__name__)
        
    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance
        
    def initialize(self,config):
        pass
    
    def _keeper_logger_register(self, client=None):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        for logging_config in self._settings['LOGGING_CONFIG_LIST']:
            handler = logging_config['logging.handler']
            formatter = logging.Formatter(logging_config['logging.formatter'])
            level = logging_config['logging.level']
            is_thrift = logging_config.get("logging.thrift", False)
            if is_thrift and client:
                handler.set_thrift_client(client)
            handler.setFormatter(formatter)
            handler.setLevel(level)
            logger.addHandler(handler)

class ClientKeeper(Keeper):
    
    def __init__(self):
        self._client = None
        self._server = None
        self._name = None
        super(ClientKeeper, self).__init__()
    
    def initialize(self,config):
        self._config = config
        self._settings = self._config.settings
        FutureThreadPool.initialize(self._settings['FUTURE_WORKER_COUNT'])
        self._processor_init()
        start_func , func_args, func_kw = self._config.read_start_func()
        send_func, end_func_args, end_func_kw = self._config.read_end_func()
        ThreadPoolProcessor.initialize(start_func, func_args, func_kw, self._settings['TASK_WORKER_COUNT'],
                                       self._settings['TASK_INTERVAL'],send_func,end_func_args,end_func_kw)
    
    @property
    def thfit_client(self):
        return self._thrift_client
    
    @property
    def thrift_server(self):
        return self._thrift_server
    
    def _processor_init(self):
        RpcClientMonitor.initialize(self._settings['ZOOKEEPER_SERVER'], self._settings['ZOOKEEPER_PATH'])
        ori_rpc_node = RpcData(self._settings['THRIFT_CLIENT_HOST'], self._settings['THRIFT_CLIENT_NAME'])
        RpcClientMonitor.add_register_callback(ori_rpc_node, self._crawler_init)
#         self._thread_pool_processor = thread_pool_processor
        
    def _crawler_init(self, rpc_node):
        client_host, client_port = rpc_node.server_h.split(":")
        server_host, server_port = rpc_node.client_h.split(":")
        self._crawler_server_start(server_host, server_port)
        self._crawler_client_start(client_host, client_port, self._settings['THRIFT_CLIENT_NAME'])
        
    def _crawler_server_start(self, host, port):
        if self._server:
            if self._server.is_running() and not self._server.is_same_conn(host, port):
                self._server.stop()
            else:
                return
        self._server = CrawlerClientServer(host, port)
        self._server.start()
        
    def _crawler_client_start(self, host, port, name):
        self._client = CrawlerThriftClient(host, port, name)
        self._keeper_logger_register(self._client)
        
        
class CenterKeeper(Keeper):
    
    def __init__(self):
        self._client_cluster = {}
        self._server = None
        self._name = None
        super(CenterKeeper, self).__init__()
        
    def initialize(self,config):
        self._config = config
        self._settings = self._config.settings
        self._redis_pool_init()
        self._keeper_logger_register()
        self._processor_init()
    
    def _redis_pool_init(self):
        redis_host_config = self._settings['REDIS_SERVER_HOST']
        r_host,r_port = redis_host_config.split(":")
        RedisManager.initialize(RedisPool(r_host,int(r_port),self._settings['REDIS_SERVER_DB']))
        self._redis_manager = RedisManager.instance()
    
    @property
    def client_cluster(self):
        return self._client_cluster
    
    def _center_server_start(self, host, port):
        server = CenterClientServer(host, int(port))
        self._server = server
        server.start()
    
    def _client_cluster_reload(self, cluster_nodes):
        
#         changedKeys = set(cluster_nodes.keys())
#         existKeys = set()
#         with self._rpcRwLock.readLock:
#             cluster = self._rpcRepo.get(service)
#             if cluster is not None:
#                 existKeys = set(cluster.keys())
#         
#         # calculate intersection rpc nodes
#         same = changedKeys.intersection(existKeys)
#         for k in same:
#             changedKeys.remove(k)
#             existKeys.remove(k)
#         
#         for name in changedKeys:
#             self.register_rpc_backend(service, name, changed[name])
#             
#         for name in existKeys:
#             self.unregister_rpc_backend(service, name)
        
        changed_keys = set(cluster_nodes.keys())
        exist_keys = set()
        
        with self._rpcRwLock.readLock:
            if self._client_cluster is not None:
                exist_keys = set(self._client_cluster.keys())
        same = changed_keys.intersection(exist_keys)
        for k in same:
            rpc_node = cluster_nodes[k]
            old_client = self._client_cluster[k]
            if ":".join((old_client.host,str(old_client.port),old_client.name)) == ":".join((rpc_node.client_h,rpc_node.client_n)):
                changed_keys.remove(k)
                exist_keys.remove(k)
                continue
        
        for name in cluster_nodes:
            rpc_node = cluster_nodes[name]
            host, port = rpc_node.client_h.split(":")
            client = CenterThriftClient(host, int(port), rpc_node.client_n)
            self._client_cluster[name] = client
            
        for name in exist_keys:
            self._client_cluster.pop(name)
        
#         for servicename, rpc_node in cluster_nodes.items():
#             if servicename in self._client_cluster:
#                 old_client = self._client_cluster[servicename]
#                 if ":".join((old_client.host,str(old_client.port),old_client.name)) == ":".join((rpc_node.client_h,rpc_node.client_n)):
#                     continue
#             host, port = rpc_node.client_h.split(":")
#             client = CenterThriftClient(host, int(port), rpc_node.client_n)
#             self._client_cluster[servicename] = client
    
    def _processor_init(self):
        thrift_server_host = self._settings['THRIFT_SERVER_HOST']
        host, port = thrift_server_host.split(":")
        self._center_server_start(host, int(port))
        RpcServerMonitor.add_register_callback(self._client_cluster_reload)
        RpcServerMonitor.initialize(self._settings['ZOOKEEPER_SERVER'], self._settings['ZOOKEEPER_PATH'],
                                    thrift_server_host, self._settings['THRIFT_SERVER_NAME'])
    
