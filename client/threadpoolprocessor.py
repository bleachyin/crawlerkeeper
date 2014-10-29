#!/usr/bin/python
# -*- coding: utf-8 -*- 
import logging
import time
import ujson
from threading import  Timer
from crawlerkeeper.common.jsonconst import *
from crawlerkeeper.client.threadpool import ThreadPool
import crawlerkeeper.common.future as future
from threading import Condition, Lock
from crawlerkeeper.common.future import FutureThreadPool

class _ThreadPoolProcessor(object):
    
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._start_time = int(time.time())

    def initialize(self,start_func,func_args,func_kw,worker_count,interval,end_func=None,end_func_args=None,end_func_kw=None):
        self._timer = Timer(interval,self.processor_start)
        self._worker_count = worker_count
        self._start_func = start_func
        self._end_func = end_func
        self._func_args = func_args
        self._end_func_args = end_func_args
        self._func_kw = func_kw
        self._end_func_kw = end_func_kw
        self._interval = interval
        self._condition = Condition(Lock())
        ThreadPool.initialize(worker_count)
        
    def _timer_reset(self):
        if self._timer:
            self._timer.cancel()
        self._timer =  Timer(self._interval,self.processor_start)
        
    def processor_future_start(self):
        flag = THRIFT_STATUS_SUCCESS
        try:
            self._start_reset()
            FutureThreadPool.add_task(self.processor_start)
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("processor_future_start occur exception due to : %s",ex)
        finally:
            return flag
        
    def processor_future_pause(self):
        flag = THRIFT_STATUS_SUCCESS
        try:
           FutureThreadPool.add_task(self.processor_pause)
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("processor_future_pause occur exception due to : %s",ex)
        finally:
            return flag

    def _start_reset(self):
        if ThreadPool.is_sleeping():
            self._restart_finished = False
        else:
            self._restart_finished = True
        ThreadPool.all_task_sleeping()
        ThreadPool.clear_task()
        while ThreadPool.is_running() and self._restart_finished:
            time.sleep(1)
        self._timer_reset()
        ThreadPool.initialize(self._worker_count)
    
    def processor_start(self):
        try:
            self._start_time = int(time.time())
            ThreadPool.all_task_running()
            if self._func_args or self._func_kw:
                self._start_func(self._func_args,self._func_kw)
            else:
                self._start_func()
            ThreadPool.wait_for_complete()
            ThreadPool.clear_task()
            if self._end_func:
                if self._end_func_args or self._end_func_kw:
                    self._end_func(self._end_func_args,self._end_func_kw)
                else:
                    self._end_func()
            if not self._restart_finished and ThreadPool.is_running():
                self._timer_reset()
                self._timer.start()
            self._restart_finished = False
        except Exception as ex:
            self._logger.error("core.processor failed due to :%s",ex,exc_info=1)
            
    def processor_pause(self):
        try:
            ThreadPool.all_task_wait()
        except Exception as ex:
            self._logger.error("processor_pause occur exception due to : %s",ex)

    def processor_stop(self):
        flag = THRIFT_STATUS_SUCCESS
        try:
            ThreadPool.all_task_stop()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("processor_stop occur exception due to : %s",ex)
        finally:
            return flag
        
    def processor_resume(self):
        flag = THRIFT_STATUS_SUCCESS
        try:
            ThreadPool.all_task_resume()
        except Exception as ex:
            flag = THRIFT_STATUS_ERROR
            self._logger.error("processor_resume occur exception due to : %s",ex)
        finally:
            return flag
        
    def processor_running_info(self):
        info = {}
        info['start_time'] =  self._start_time
        info['duration_time'] = int(time.time()) - self._start_time
        info['running_status'] = ThreadPool.status
        info['worker_count'] = self._worker_count
        info['task_interval'] = self._interval
        info['unfinished_tasks'] = ThreadPool.unfinished_tasks
        return ujson.dumps(info, ensure_ascii=False)
        
    
ThreadPoolProcessor = _ThreadPoolProcessor()