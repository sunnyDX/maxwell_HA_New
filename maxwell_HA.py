# -*- coding:utf-8 -*-
import logging, os, time
from zookeeper import ZooKeeper
import ConfigParser
# import configparser
import MySQLdb
import threading
# import pymysql

# 线程死掉的预警阈值
alear_count = 0

def getMysqlConnetcion(config, type):
    """mysql数据连接获取

    :param config：ConfigParser配置实例
    :param type: ConfigParser配置类型，共包括[mysql_maxwell][service_1]两种mysql配置类型
    :returns: mysql数据库连接
    """
    host = config.get(type, "host")
    port = int(config.get(type, "port"))
    db = config.get(type, "db")
    username = config.get(type, "username")
    passwd = config.get(type, "passwd")
    if (db != "Null"):
        conn = MySQLdb.connect(host=host, port=port, user=username, passwd=passwd, db=db, charset="utf8")
        # conn = pymysql.connect(host=host, port=port, user=username, passwd=passwd, db=db, charset="utf8")
    else:
        conn = MySQLdb.connect(host=host, port=port, user=username, passwd=passwd, charset="utf8")
        # conn = pymysql.connect(host=host, port=port, user=username, passwd=passwd, charset="utf8")
    return conn

def getBinlog_position(config, instance):
    """获取binlog 文件名称以及position

    :param config：ConfigParser配置实例
    :param instance: ConfigParser配置中的maxwell服务实例名称，共包括[service_1]-[service_5]五个服务
    :param position_rows: 基于参数指定的position_rows获取binlog 文件名称以及position
    :returns: 对应instance以及token的binlog的文件名称和position

    """
    conn = getMysqlConnetcion(config, instance)
    cursor = conn.cursor()
    try:
        if conn != None and cursor != None:
            cursor.execute("show master status;")
            row = cursor.fetchone()
            now_position = int(row[1])
            now_file = str(row[0])
            fromposition = now_position - (50 * int(config.get("times", "getposition_rows")))
            if fromposition < 4:
                # 考虑因为binlog文件切换问题需要夸binlog文件
                cursor.execute("show binary logs;")
                results = cursor.fetchall()
                resnums = len(results)
                if resnums >= 2:
                    fromposition = int(results[resnums - 2][1]) - (50 * int(config.get("times", "getposition_rows")))
                    now_file = str(results[resnums - 2][0])

                else:
                    fromposition = 4

            for i in range(1, 1000):
                try:
                    cursor.execute(
                        "show binlog events in '" + str(now_file) + "'  from   " + str(fromposition) + "  limit 10;")
                    conn.commit()
                    row = cursor.fetchone()
                    if row == None or row == '':
                        fromposition = fromposition - 1
                    else:
                        break
                except:
                    fromposition = fromposition - 1
            conn.close()
        result = {
            'status': True,
            'binlogfile': now_file,
            'position': fromposition
        }
    except:
        if conn != None:
            conn.close()
        result = {
            'status': False,
            'binlogfile': now_file,
            'position': fromposition
        }
    return result

def drop_maxwell_metadata(config, type, database):
    """删除指定的maxwell元数据库，目的是解决maxwell重启时的中断异常

    :param config: ConfigParser配置实例
    :param type: ConfigParser配置类型，共包括[mysql_maxwell][service_1]两种mysql配置类型
    :param database: 待删除的数据库名称
    """
    conn = getMysqlConnetcion(config, type)
    cursor = conn.cursor()
    try:
        if conn != None and cursor != None:
            result = cursor.execute("drop database if exists " + database + ";")
            conn.commit()
            if (result != 0):
                print("删除maxwell元数据库：" + database)
            conn.close()
    except:
        if conn != None:
            conn.close()

def send_message(config, content):
    """短信预警

    :param config：ConfigParser配置实例
    :param content：content预警内容
    """
    warn_list = []
    warn_list.extend(str(config.get("alert", "alert_list")).split(","))
    for info in warn_list:
        phone = str(info)
        command = "curl \"http://api.xueersi.com/yunweimsg/sendMsg?phone=" + str(
            phone) + "& note=" + content + "\" >> /dev/null 2>&1 &"
        if (os.system(command) == 0):
            print("短信预警发送成功！")

def start_maxwell_1(zk, instance, logger ,flag):
    """基于is_auto状态值，启动不同配置的maxwell服务

    :param instance: configParser配置中的maxwell服务实例名称，共包括[service_1]-[service_5]五个服务
    :param position_rows: 基于position_rows获取新的position启动重定位的maxwell
	:param logger：log日志类
    """
    # 如果是第一次初始化,则启动非重定位的maxwell
    if (int(zk.getData("is_auto/" + instance)) == 0 or flag == 1):
        logger.info(instance + " 初始化操作, 并触发启动非重定位的maxwell")
        # 启动maxwell
        os.system(
            "cd /data/maxwell-1.5.0/ && bin/maxwell --config conf/" + instance + ".properties --kafka_partition_by=table >> /dev/null 2>&1 &")
    # 否则请求锁，并启动重定位的maxwell
    else:
        logger.info(instance + " 非初始化操作，并触发启动重定位的maxwell")
        result = getBinlog_position(config, instance)
        position = result['position']
        binlog = result['binlogfile']
        # 删除原有的maxwell 元数据库，防止异常产生
        client_id = config.get(instance, "client_id")
        database = "maxwell_" + client_id
        drop_maxwell_metadata(config, "mysql_maxwell", database)
        # 启动maxwell
        os.system(
            "cd /data/maxwell-1.5.0/ && bin/maxwell --config conf/" + instance + ".properties" + " --init_position " + binlog + ":" + str(
                position) + " --kafka_partition_by=table >> /dev/null 2>&1 &")

def start_maxwell_2(instance, logger, init_position):
    """基于init_position参数，启动maxwell服务

    :param instance: configParser配置中的maxwell服务实例名称，共包括[service_1]-[service_5]五个服务
    :param init_position: 基于init_position参数启动重定位的maxwell
    :param logger：log日志类
    """
    logger.info(instance + " 非初始化操作，人为指定init_position并触发启动重定位的maxwell")
    # 删除原有的maxwell 元数据库，防止异常产生
    client_id = config.get(instance, "client_id")
    database = "maxwell_" + client_id
    drop_maxwell_metadata(config, "mysql_maxwell", database)
    # 启动maxwell
    os.system(
        "cd /data/maxwell-1.5.0/ && bin/maxwell --config conf/" + instance + ".properties" + " --init_position " + init_position + " --kafka_partition_by=table >> /dev/null 2>&1 &")

def action_maxwell(zk, config, logger, instance):
    """maxwell触发线程函数

    :param config：ConfigParser配置实例
    :param logger：log日志类
    :param instance: ConfigParser配置中的maxwell服务实例名称，共包括[service_1]-[service_5]五个服务
	:param flag: 如果线程死掉后，则重启该线程，并通过flag标志位保证启动重定位maxwell

    """
    timeout = int(config.get("validation", "timeout"))
    client_id = config.get(instance, "client_id")
    validation_path = config.get("validation", "register_path")
    if (zk.is_exists(validation_path + "/" + client_id) == None):
        start_maxwell_1(zk, instance, logger,flag=1)

    count = 0  # 统计自动修复服务次数
    is_custom = 0
    while (True):
        time.sleep(timeout)
        if (zk.is_exists(validation_path + "/" + client_id) == None):
        # if (os.system("ps -ef | grep " + instance + " | grep -v grep | grep -v cd >> /dev/null") != 0):
            if (count <= 3):
                count += 1
            # 如果尝试自动修复服务三次不成功的话，需要人为修复，并指定新的init_position
            if (int(zk.getData("custom/" + instance)) == 0 and count <= 3):
                content = "尝试自动修复" + str(instance) + "服务" + str(count) + "次！"
                logger.info(content)
                send_message(config, content)
                start_maxwell_1(zk, instance, logger,flag = 0)
            if (count == 3):
                content = "尝试自动修复" + str(instance) + "服务3次失败，请人为修复！"
                logger.info(content)
                send_message(config, content)
            if (int(zk.getData("custom/" + instance)) == 1):
                init_position = str(zk.getData("custom/init_position/" + instance))
                start_maxwell_2(instance, logger, init_position)
                if (is_custom == 0):
                    is_custom = 1
                    continue
                content = "自定义修复" + str(instance) + "服务失败，请再次注意修复！"
                logger.info(content)
                send_message(config, content)
        else:
            count = 0
            is_custom = 0
            alear_count = 0

if __name__ == "__main__":
    # ------------------------------------------------------------
    # Logger设置
    # ____________________________________________________________
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s -%(module)s:%(filename)s-L%(lineno)d-%(levelname)s: %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    # ------------------------------------------------------------
    # 配置获取
    # ____________________________________________________________
    # config = configparser.ConfigParser()
    config = ConfigParser.ConfigParser()
    # config.read("config.conf",encoding="utf-8")
    config.read("config.conf")
    # ------------------------------------------------------------
    # 请求共享锁（阻塞）
    # ____________________________________________________________
    logger.info("本机正在请求共享锁...........")
    zk_lock = ZooKeeper(config, logger)
    zk_lock.create_lock("locks/service")
    is_acquired = zk_lock.acquireLock()
    if (is_acquired == True):
        logger.info("本机获取共享锁成功！!")
    # ------------------------------------------------------------
    # 1> 设置多线程
    # ------------------------------------------------------------
    zk = ZooKeeper(config, logger)
    instances = []
    instances.extend(str(config.get("zookeeper", "instance_list")).split(","))
    threads = []
    for ins in instances:
        t = threading.Thread(target=action_maxwell, args=(zk, config, logger, ins),
                             name=ins)
        threads.append(t)
    # ------------------------------------------------------------
    # 2> 启动守护线程
    # ------------------------------------------------------------
    for t in threads:
        t.setDaemon(True)
        t.start()
        time.sleep(5)
    # ------------------------------------------------------------
    # 3> 监控子线程的存活状态，一旦死掉则触发重启
    # ------------------------------------------------------------
    while (True):
        time.sleep(5)
        for t in threads:
            if (t.isAlive() == False):
                content = t.name + "线程死掉，已尝试重启！"
                logger.info(content)
                t = threading.Thread(target=action_maxwell, args=(zk, config, logger, ins), name=ins)
                t.setDaemon(True)
                t.start()
                if (alear_count < 3):
                    send_message(config, content)
                    alear_count += 1
    logger.info("主线程结束!")
