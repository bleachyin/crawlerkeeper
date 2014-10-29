# -*- coding: utf-8 -*- 
#

import logging
import time
import Queue, threading
from threading import Condition, Lock
from threading import Thread
from crawlerkeeper.common.jsonconst import THREAD_POOL_STATUS_RUNNING, THREAD_POOL_STATUS_STOP, \
    THREAD_POOL_STATUS_PAUSE, THREAD_POOL_STATUS_SLEEPING

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
                param = callback[1]
                if task is None:
                    continue
                try:
                    if param is not None:
                        task(*param)
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
        
class _ThreadPool(object):
    
    def __init__(self):
        self._initialized = False
        self._condition = Condition(Lock())        
        self._status = THREAD_POOL_STATUS_SLEEPING
    
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
        self.__workerCount = workerCount
        self.__workers = []
        for i in range(self.__workerCount):
            worker = _Worker("_Worker-" + str(i + 1), self.__workQueue, self)
            worker.start()
            self.__workers.append(worker)
        self._initialized = True
            
    def stop(self):
        ''' Wait for each of them to terminate'''
        self._status = THREAD_POOL_STATUS_STOP
        while self.__workers:
            worker = self.__workers.pop()
            worker.stop = True
#             self.__workQueue.put(None)
        self._initialized = False
            
    def add_task(self, callback):
        if not self._initialized:
            raise AttributeError("WorkerManager does not initialized before calling add_task method")
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
#            self.__logger.info("add_task: num:%d", self._unfinished_tasks)
            self._condition.release()            
            self.__workQueue.put((callback, None))
        
    def add_task_with_param(self, callback, param):
        if not self._initialized:
            raise AttributeError("WorkerManager does not initialized before calling add_task method")
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
#            self.__logger.info("add_task_with_param: num:%d", self._unfinished_tasks)
            self._condition.release()
            self.__workQueue.put((callback, param))
        
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
#         self._condition.notify_all()
        self._condition.release()            
        
    def wait_for_complete(self, timeout=None):
        start = time.time()
        self._condition.acquire()
        try:
            while self._unfinished_tasks:
                self._condition.wait(30)
                if timeout is not None:
                    current = time.time()
                    if (current - start) > timeout:
                        self.__logger.error('Thread pool timeout')
                        break
        finally:
            self._condition.release()
            
    def all_task_wait(self):
        self.__logger.warn("crawler status is : %s", self._unfinished_tasks)
        if self._status != THREAD_POOL_STATUS_RUNNING:
            return
        self._condition.acquire()
        try:
            self._status = THREAD_POOL_STATUS_PAUSE
            while self._status == THREAD_POOL_STATUS_PAUSE:
                self.__logger.warn("crawler status [blocking] thread pool has unfinished task : %s", self._unfinished_tasks)
                time.sleep(1)  # sleep 5s
        except Exception as ex:
            self._status = THREAD_POOL_STATUS_RUNNING
            self.__logger.error("exec all thread wait failed due to: %s", str(ex))
        finally:
            self._condition.release()
           
    def all_task_resume(self):
        if self._status == THREAD_POOL_STATUS_STOP or self._status == THREAD_POOL_STATUS_SLEEPING:
            return
        self._status = THREAD_POOL_STATUS_RUNNING
        
    def all_task_running(self):
        self._status = THREAD_POOL_STATUS_RUNNING
        
    def all_task_sleeping(self):
        self._status = THREAD_POOL_STATUS_SLEEPING
           
    def all_task_stop(self):
        self.stop()
        self.clear_task()
        
    def is_running(self):
        return self._status == THREAD_POOL_STATUS_RUNNING
       
    def is_sleeping(self):
        return self._status == THREAD_POOL_STATUS_SLEEPING
    
    @property
    def status(self):
        return self._status
    
    @property
    def unfinished_tasks(self):
        return self._unfinished_tasks
        
        
ThreadPool = _ThreadPool.instance()
