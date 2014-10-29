# -*- coding: utf-8 -*- 
#
import os
from datetime import datetime
from time import strptime
from conf.consts import POSTER_STORE_PATH
import logutil

class Utils(object):

    @classmethod
    def getTagText(cls,root, tag):
        node = root.getElementsByTagName(tag)
        if node:
            node = node[0]
        else:
            return None
        
        rc = ""
        for node in node.childNodes:
            if node.nodeType in (node.TEXT_NODE, node.CDATA_SECTION_NODE):
                rc = rc + node.data
        return str(rc.strip()) 
    
    @classmethod
    def getTagListText(cls, root, tag):
        result = []
        nodeList = root.getElementsByTagName(tag)
        for node in nodeList:
            rc = ""
            for child in node.childNodes:
                if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE):
                    rc = rc + child.data
            if rc.strip():
                result.append(str(rc.strip()))
        return result
    
    @classmethod
    def isPosterExist(cls, posterUrl):
        try:
            year,month,day,posterName = Utils.getInfo(posterUrl)
            posterFullFile = os.path.join(POSTER_STORE_PATH, year, month, day, posterName)
        except Exception as e:
            logutil.logger.error("isPosterExist for posterurl %s error: %s", posterUrl, e)
            return True    
  
        if os.path.exists(posterFullFile):
            return True
        else:
            return False

    @classmethod
    def getInfo(cls, posterUrl):
        strSeq = posterUrl.split('/')
        dateString = strSeq[3]
        year = dateString[0:4]
        month = dateString[4:6]
        day = dateString[6:8]
        picName = strSeq[4]
        return year, month, day, picName     
        
    @classmethod
    def convert_str_to_datetime(cls, dateTimeStr):
        return datetime(*strptime(dateTimeStr, '%Y-%m-%d %H:%M:%S')[0:6])
    
    @classmethod
    def convert_datetime_to_str(cls, dateTime):
        return '%04d-%02d-%02d %02d:%02d:%02d' % (dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute, dateTime.second)