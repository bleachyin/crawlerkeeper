# -*- coding: utf-8 -*- 
#

import logging
import time
import Queue, threading
from threading import Condition, Lock
from threading import Thread
from functools import wraps

class _Worker(Thread):
    '''
    worker thread which get task from queu to execute
    '''      

    def __init__(self, threadname, workQueue, parent):
        threading.Thread.__init__(self, name=threadname)
        self.__logger = logging.getLogger(threadname)
        self.__parent = parent
        self.__workQueue = workQueue
        self.stop = False
        
    def run(self):
        while not self.stop:
            try:
                callback = self.__workQueue.get()
                task = callback[0]
                args_param = callback[1]
                kw_param = callback[2]
                if task is None:
                    continue
                try:
                    if args_param or kw_param:
                        task(args_param,kw_param)
                    else:
                        task()
                except Exception as processEx:
                    self.__logger.error("%s execute callback: %r failed due to %s", self.name, callback, str(processEx))
                finally:
                    self.__parent.complete_task()
            except IOError:
                pass
            except Exception as getEx:
                self.__logger.error("%s get task from queue failed: %s", self.name, getEx)
        
class _FutureThreadPool(object):
    
    def __init__(self):
    	self._initialized = False
    
    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
            cls.__logger = logging.getLogger("thread_pool")
        return cls._instance
    
#     def initialize(self, workerCount=HANDLER_THREAD_COUNT):
    def initialize(self, workerCount):
        self.__workQueue = Queue.Queue()
        self._unfinished_tasks = 0
        self._condition = Condition(Lock())        
        self.__workerCount = workerCount
        self.__workers = []
        for i in range(self.__workerCount):
            worker = _Worker("_Worker-" + str(i + 1), self.__workQueue, self)
            worker.start()
            self.__workers.append(worker)
       	self._initialized = True
            
    def stop(self):
        ''' Wait for each of them to terminate'''
        while self.__workers:
            worker = self.__workers.pop()
            worker.stop = True
        self._initialized = False
            
    def add_task(self, callback):
    	if not self._initialized:
            raise AttributeError("WorkerManager does not initialized before calling add_task method")
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
#            self.__logger.info("add_task: num:%d", self._unfinished_tasks)
            self._condition.release()            
            self.__workQueue.put((callback, None,None))
        
    def add_task_with_param(self, callback, *args_param,**kw_param):
     	if not self._initialized:
    	    raise AttributeError("WorkerManager does not initialized before calling add_task method")
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
#            self.__logger.info("add_task_with_param: num:%d", self._unfinished_tasks)
            self._condition.release()
            self.__workQueue.put((callback, args_param,kw_param))
        
    def complete_task(self):
        self._condition.acquire()
        try:
            unfinished = self._unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    self.__logger.error('task_done() called too many times')
                self._condition.notify_all()
            self._unfinished_tasks = unfinished            
            self.__logger.info('Thread pool has %d unfinished tasks', self._unfinished_tasks)
        finally:
            self._condition.release()
            
    def clear_task(self):
        self._condition.acquire()        
        while not self.__workQueue.empty():
            try:
                task = self.__workQueue.get()
                self._unfinished_tasks -= 1
                self.__logger.error('Thread pool has unfinished task:%s', str(task))
            except Exception as ex:
                self.__logger.error("%s clear_task: %r failed due to %s", self.name, str(ex))
        self._condition.release()            
        
    def wait_for_complete(self, timeout=None):
        start = time.time()
        self._condition.acquire()
        try:
            while self._unfinished_tasks:
                self._condition.wait(10)
                if timeout is not None:
                    current = time.time()
                    if (current - start) > timeout:
                        self.__logger.error('Thread pool timeout')
#                         break
        finally:
            self._condition.release()

FutureThreadPool = _FutureThreadPool.instance()