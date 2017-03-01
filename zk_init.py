#coding=utf-8
from kazoo.client import KazooClient
zk = KazooClient(hosts="10.10.8.95:2181")
zk.start()
if __name__ == '__main__':
    services = ["service_1", "service_2", "service_3", "service_4", "service_5"]
    for instance in services:
        # 初始化is_auto
        zk.ensure_path(path="/maxwell/is_auto/" + instance)
        zk.set(path="/maxwell/is_auto/"+instance, value="1".encode("utf-8"))
        # 初始化locks
        zk.ensure_path(path="/maxwell/locks/service")
        # 初始化custom
        zk.ensure_path(path="/maxwell/custom/" + instance)
        zk.set(path="/maxwell/custom/"+instance, value="0".encode("utf-8"))
        # 初始化init_position
        zk.ensure_path(path="/maxwell/custom/init_position/" + instance)
        zk.set(path="/maxwell/custom/init_position/"+instance, value="binlog000:000".encode("utf-8"))
    zk.stop()
    zk.close()