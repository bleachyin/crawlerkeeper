#!/usr/bin/python
# -*- coding: utf-8 -*- 
#
from conf import settings
from settings import CrawlerSettings
from client.clientmanager import ClientManager
from server.thriftserver import Server
from client.thriftlogger import ThriftLogger
import logging

#test setting ok
# settings = CrawlerSettings(settings)
# print settings['THRIFT_SERVER']

# ClientManager(settings).initialize()
# rclogger = logging.getLogger("rr")
# # rclogger.setLevel(logging.INFO)
# rclogger.info("info")

# 
# def r():
#     print "running"
#     
# t = Timer(3,r)
# t.start()
# def r():
#     print "running"
#     t = Timer(1,r)
#     t.start()
# r()
        