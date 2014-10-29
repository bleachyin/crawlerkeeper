'''
Created on 2013-8-13

@author: liang xiaofeng
'''
import logging.handlers
from client.thriftlogger import ThriftLogger

logging.setLoggerClass(ThriftLogger)

formatter = logging.Formatter('[%(asctime)s][module:%(module)s][lineno:%(lineno)d][%(levelname)s]: %(message)s')

console = logging.StreamHandler()  
console.setFormatter(formatter)
console.setLevel(logging.INFO) 

file1 = logging.handlers.TimedRotatingFileHandler(filename="juren.log", interval = 2, backupCount=5)
file1.setFormatter(formatter)
file1.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console)
logger.addHandler(file1)
