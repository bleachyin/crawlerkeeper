#!/usr/bin/python
# -*- coding: utf-8 -*- 

#!/usr/bin/env python
import logging
import logging.handlers
import crawlerkeeper.logger as logger

ZOOKEEPER_SERVER = "127.0.0.1:2181"
ZOOKEEPER_PATH = "/mitv/crawler"

THRIFT_DEBUG = True
THRIFT_SERVER="localhost:6888"
# THRIFT_SERVER= 6888

LOGGING_CONFIG_LIST = [
               {
                "logging.formatter":"[%(asctime)s][module:%(module)s][lineno:%(lineno)d][%(levelname)s]: %(message)s",
                "logging.level":logging.INFO,
                "logging.handler":logging.StreamHandler()
                },
               {
                "logging.formatter":"[%(asctime)s][module:%(module)s][lineno:%(lineno)d][%(levelname)s]: %(message)s",
                "logging.level":logging.INFO,
                "logging.handler":logging.handlers.TimedRotatingFileHandler(filename="thrift.log", interval = 2, backupCount=5)
                },
                {"logging.formatter":"[%(asctime)s][module:%(module)s][lineno:%(lineno)d][%(levelname)s]: %(message)s",
                 "logging.level":logging.INFO,
                 "logging.handler":logger.handler.ThriftHandler(),
                 "logging.thrift":True,
                 }]
