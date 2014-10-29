#!/usr/bin/python
# coding: utf-8
#
# Copyright 2011 DuoKan
#
from crawlerkeeper.settings import Settings
import logging

class BaseConfig(object):
    
    def __init__(self,settings=None):
        if not settings :
            self._settings = Settings()
        else:
            self._settings = settings
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def settings(self):
        return self._settings

class CenterConfig(BaseConfig):
    
    def __init__ (self,settings = None):
        super(CenterConfig, self).__init__(settings)

class CrawlerConfig(BaseConfig):
    
    def __init__(self,settings=None):
        self._start_func = None
        self._func_args = None
        self._func_kw = None
        self._end_func = None
        self._end_func_args = None
        self._end_func_kw = None
        super(CrawlerConfig, self).__init__(settings)
    
    def read_start_func(self):
        return self._start_func,self._func_args,self._func_kw
    
    def read_end_func(self):
        return self._end_func,self._end_func_args,self._end_func_kw
    
    def add_start_func(self,func,*args,**kw):
        self._start_func = func
        self._func_args = args
        self._func_kw = kw
        
    def add_end_func(self,func,*args,**kw):
        self._end_func = func
        self._end_func_args = args
        self._end_func_kw = kw
