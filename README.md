### mysql binlog同步数据到Kafka,通过zookeeper实现maxwell高可用

***
目前数据为一主两从，每组有5个实例,共享一个VIP，共三个VIP，具体关系图如下：


               （主库）       （备库1）        （备库2）

 service_1      ip:port         ip:port         ip:port (共享VIP1)

 service_2      ip:port         ip:port         ip:port (共享VIP2)

 service_3      ip:port         ip:port         ip:port (共享VIP3)

 service_4      ip:port         ip:port         ip:port (共享VIP4)

 service_5      ip:port         ip:port         ip:port (共享VIP5)

***

> 目前方案做出如下策略：
在服务器1上共启动service_1———service_5五个服务实例，分别对应五个VIP对mysql-binlog实施数据同步。当任何一个实例服务过程中出现问题时，则会进行3次自动修复，3次自动修复不成功的话，则需要发送短信预警，需人为干涉进行自定义修复。同时在2、3服务器上设置监控进程，与1监控一个共享锁，一旦1机器宕机，或者其他异常出现，则2或者3会获取到共享锁，并启动正常的maxwell服务。

***

#### zookeeper  目录说明：

**工作根目录： /maxwell**


1. /registerpath: maxwell注册服务

 说明: 一旦maxwell正常启动，在会在此目录下建立相应以client_id命名的的临时目录；

 当maxwell出现异常，则对应的目录则会立即消失。

2. /custom: 是否自定义启动自定义修复

  说明：该目录下共有service_1———service_5五个文件，用于在系统自动修复maxwell服务不成功的情况下，确定service_1———service_5每个服务是否需要人工手动干   涉。一旦需要人工干涉，则需设人为设置init_position

默认值: 0（即不需要人工干涉）

3. /custom/init_position: 在自定义修复启动的条件下，设置service_1 -- service_5五个实例的init_position的值

说明：在目录下也有共有service_1———service_5五个文件，在设置/registerpath/custom为1的情况下，需将对应需要人工干涉修复的服务对应的service_?的值设置为（binlog文件：position）格式的数值

默认值：其他

4. /locks/service: 共享锁

说明: 此目录下正常情况下会有三条数据，分别代表147、148、149三台服务物理机竞争一个共享锁。

正常情况下会有一个物理机的maxwell处于服务状态，其余两个处于监控阻塞状态，一旦获取到该共享锁，则会由该台物理机提供maxwell服务

5. /is_auto: 是否是自动启动

说明：手动启动：一般用于人工异常修复

 自动启动：一般用于异常自动切换

 默认：一开始是手动启动（启动成功后，则需设置成自动启动）
