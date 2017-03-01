# -*- coding:utf-8 -*-
import logging, os, time
from kazoo.recipe.lock import Lock
from kazoo.client import KazooClient
class ZooKeeper():
    """.
    zookeeper操作类
    Attributes:
        hosts: zookeeper 主机地址及端口
        workers_path: zookeeper的默认工作根目录
        timeout: 超时时间
        logger: log日志类
        lock_handle: zookeeper的共享锁
        zk_client：zookeeper的客户端

    """

    def __init__(self, config, logger=None, timeout=10):
        """初始化ZooKeeper类

        """
        self.hosts = str(config.get("zookeeper", "hosts"))
        self.workers_path = str(config.get("zookeeper", "workers_path"))
        self.timeout = timeout
        self.logger = logger
        self.lock_handle = None
        self.zk_client = None
        self.__init_zk()

    def __init_zk(self):
        """创建zookeeper客户端，并开启连接

        """
        try:
            self.zk_client = KazooClient(hosts=self.hosts, logger=self.logger, timeout=self.timeout)
            self.zk_client.start(timeout=self.timeout)
        except Exception as ex:
            self.init_ret = False
            self.err_str = "zookeeper客户端连接失败! 异常: %s" % str(ex)
            logging.error(self.err_str)
            return

    def create_lock(self, lock_path):
        """创建zookeeper共享锁

        """
        try:
            self.lock_handle = Lock(self.zk_client, self.workers_path + "/" + lock_path)
        except Exception as ex:
            self.init_ret = False
            self.err_str = "创建共享锁失败！异常: %s" % str(ex)
            logging.error(self.err_str)
            return

    def is_exists(self,node):
        """判断节点是否存在

        """
        return self.zk_client.exists(self.workers_path + "/" + node)

    def close(self):
        """关闭zookeeper客户端

        """
        if self.zk_client != None:
            self.zk_client.stop()
            self.zk_client.close()

    def acquireLock(self, blocking=True, timeout=None):
        """请求获取zookeeper共享锁

        """
        if self.lock_handle == None:
            return None
        try:
            return self.lock_handle.acquire(blocking=blocking, timeout=timeout)
        except Exception as ex:
            self.err_str = "请求共享锁失败! 异常: %s" % str(ex)
            logging.error(self.err_str)
            return None

    def releaseLock(self):
        """释放zookeeper共享锁

        """
        if self.lock_handle == None:
            return None
        return self.lock_handle.release()

    def setData(self, node, value):
        """设置zookeeper指定节点的值

        """
        self.zk_client.ensure_path(self.workers_path + "/" + node)
        self.zk_client.set(path=self.workers_path + "/" + node, value=value.encode("utf-8"))

    def getData(self, node):
        """获取zookeeper指定节点的值

        """
        data, stat = self.zk_client.get(path=self.workers_path + "/" + node)
        return data.decode("utf-8")