# CrawlerKeeper


###A monitor of crawler write by [python](http://www.python.org/)

---------
Description

- CrawlerKeeper是基于zookeeper以及thrift实现的python爬虫监控框架

- CrawlerKeeper底层是基于thrift的rpc接口进行通信,当爬虫通过zookeeper注册节点被服务端获取并且响应后，爬虫客户端会根据服务器端在zookeeper注册节点内的thriftserver信息(ip地址以及端口号)生成相应的thriftclient,同时每个爬虫客户端生成一个 thriftserver, crawlercenter则会针对每个注册的爬虫客户端生成对应的thriftclient,从而达到双向通信的目的。

# Installation

 * [python2.6](http://www.python.org) 以上
 * [thrift-0.9,1](http://thrift.apache.org/)
 * [zookeeper](http://zookeeper.apache.org/)
 
 ```javascript
 root@root:~# tar -xzvf crawlerkeeper.tar.gz
 root@root:~# cd crawlerkeeper
 root@root:~# sudo python setup.py install
 ```
 
# Document
![image](http://image.box.xiaomi.com/mfsv2/download/s010/p01blhfKGYR3/uzw18E7TrejVkA.jpg)
auth & report to bug [dongchenxi@xiaomi.com](http://image.box.xiaomi.com/mfsv2/download/s010/p01blhfKGYR3/uzw18E7TrejVkA.jpg)


----------
 ![image](http://image.box.xiaomi.com/mfsv2/download/s010/p01HYtmMj49e/g0DcsHAxyqvKSF.png)
 ![image](http://image.box.xiaomi.com/mfsv2/download/s010/p01nu0ccKWdA/soPspvseEROHHd.png)
 ![image](http://image.box.xiaomi.com/mfsv2/download/s010/p01UnrOoUv9T/QMUEBIfXcjtjb7.png)
 ![image](http://image.box.xiaomi.com/mfsv2/download/s010/p01AaHTB8kQ3/kMV5ng7A7BtK8J.png)
 
