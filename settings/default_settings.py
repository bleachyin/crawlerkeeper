import logging

# ZK_CONF = {"server": "127.0.0.1:2181",
#            "path": {
#             "/mitv/crawler":RPC_TVSERVICE_NAME
#            }}

ZOOKEEPER_SERVER = "127.0.0.1:2181"
ZOOKEEPER_PATH = "/mitv/crawler"

THRIFT_DEBUG = True
THRIFT_SERVER_HOST="localhost:6888"
THRIFT_SERVER_NAME = "crawlerkeeper"

THRIFT_CLIENT_HOST = "localhost:6999"
THRIFT_CLIENT_NAME = "crawler-hash"

TASK_INTERVAL = 60*60
TASK_TIMEOUT = 60*60
TASK_WORKER_COUNT = 2

FUTURE_WORKER_COUNT = 5

REDIS_SERVER_HOST = "localhost:6379"
REDIS_SERVER_DB = 1

LOGGING_CONFIG_LIST = [
               {
                "logging.formatter":"[%(asctime)s][module:%(module)s][lineno:%(lineno)d][%(levelname)s]: %(message)s",
                "logging.level":logging.INFO,
                "logging.handler":logging.StreamHandler()
                }]
