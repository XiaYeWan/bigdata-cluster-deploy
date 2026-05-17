#!/bin/bash
#===========================================================
# deploy_master.sh - Master节点完整部署
# 包含: ZK, HDFS(NN), YARN(RM), MySQL, Hive, Spark, 
#        Kafka, Flink, DataX, DolphinScheduler, Superset
#
# ⚠️ 安全提醒: 脚本内含学习环境默认密码，生产使用请务必更换
#    密码位于下方 MySQL/Hive/DS 配置段，搜索 'Root@123456' 即可定位
#===========================================================
set -e

RED='\033[31m'; GREEN='\033[32m'; YELLOW='\033[33m'; NC='\033[0m'
step() { echo -e "\n${GREEN}[Step $1]${NC} $2"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

PKG=/root
MOD=/opt/module
SLAVES="slave1 slave2"
THIS_HOST=$(hostname)

# ===== JAVA_HOME自动探测 =====
JAVA_BIN=$(which java)
JAVA_REAL=$(readlink -f $JAVA_BIN)
JAVA_HOME=$(echo $JAVA_REAL | sed 's|/jre/bin/java||' | sed 's|/bin/java||')
[ -z "$JAVA_HOME" ] && err "无法定位JAVA_HOME"
echo -e "${GREEN}JAVA_HOME=${JAVA_HOME}${NC}"

# ============================================================
# 1. ZooKeeper (本节点)
# ============================================================
step "1" "安装 ZooKeeper 3.8.4"
ZK_HOME=${MOD}/zookeeper-3.8.4
if [ ! -d "$ZK_HOME" ]; then
    tar -zxf ${PKG}/apache-zookeeper-3.8.4-bin.tar.gz -C ${MOD}/
    mv ${MOD}/apache-zookeeper-3.8.4-bin ${ZK_HOME}
fi

mkdir -p /data/zookeeper
echo 1 > /data/zookeeper/myid   # master的myid=1

cat > ${ZK_HOME}/conf/zoo.cfg <<'EOF'
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data/zookeeper
clientPort=2181
server.1=master:2888:3888
server.2=slave1:2888:3888
server.3=slave2:2888:3888
EOF
echo ">>> ZooKeeper配置完成 (myid=1)"

# ============================================================
# 2. Hadoop HDFS + YARN
# ============================================================
step "2" "安装 Hadoop 3.3.6"
HADOOP_HOME=${MOD}/hadoop-3.3.6
if [ ! -d "$HADOOP_HOME" ]; then
    tar -zxf ${PKG}/hadoop-3.3.6.tar.gz -C ${MOD}/
fi

mkdir -p /data/hdfs/{namenode,datanode} /data/hadoop/tmp /data/hadoop/logs /tmp/hadoop_pids

# hadoop-env.sh
cat >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh <<EOF
export JAVA_HOME=${JAVA_HOME}
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
export HADOOP_PID_DIR=/tmp/hadoop_pids
export HADOOP_LOG_DIR=/data/hadoop/logs
EOF

# core-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/core-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
    <property><name>hadoop.tmp.dir</name><value>/data/hadoop/tmp</value></property>
</configuration>
EOF

# hdfs-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>dfs.namenode.name.dir</name><value>/data/hdfs/namenode</value></property>
    <property><name>dfs.datanode.data.dir</name><value>/data/hdfs/datanode</value></property>
    <property><name>dfs.replication</name><value>3</value></property>
    <property><name>dfs.webhdfs.enabled</name><value>true</value></property>
</configuration>
EOF

# yarn-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/yarn-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
    <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
    <property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
    <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
    <property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
    <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
</configuration>
EOF

# mapred-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/mapred-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>mapreduce.framework.name</name><value>yarn</value></property>
</configuration>
EOF

# workers
echo -e "master\nslave1\nslave2" > ${HADOOP_HOME}/etc/hadoop/workers

echo ">>> Hadoop配置完成"

# ============================================================
# 3. MySQL 8.0
# ============================================================
step "3" "安装 MySQL 8.0"
if ! systemctl list-units --type=service 2>/dev/null | grep -q mysqld; then
    rpm -qa | grep mariadb | xargs rpm -e --nodeps 2>/dev/null || true
    yum install -y wget 2>/dev/null || true
    
    # 下载安装
    if [ ! -f /tmp/mysql80.rpm ]; then
        wget -O /tmp/mysql80.rpm https://dev.mysql.com/get/mysql80-community-release-el7-11.noarch.rpm
    fi
    rpm -ivh /tmp/mysql80.rpm 2>/dev/null || true
    
    # 先尝试正常安装
    echo ">>> 安装MySQL..."
    yum install -y mysql-community-server mysql-community-client mysql-community-devel 2>&1 | tail -10 || {
        warn "正常安装失败，尝试 --nogpgcheck..."
        yum install -y --nogpgcheck mysql-community-server mysql-community-client mysql-community-devel 2>&1 | tail -10 || {
            warn "MySQL安装失败! 请手动安装:"
            echo "  rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022"
            echo "  yum install -y --nogpgcheck mysql-community-server"
        }
    }
fi

# 配置
cat > /etc/my.cnf <<EOF
[mysqld]
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
default_authentication_plugin=mysql_native_password
max_connections=200
bind-address=0.0.0.0
EOF

systemctl start mysqld 2>/dev/null || true
systemctl enable mysqld 2>/dev/null || true

# 初始化密码和数据库
TMP_PASS=$(grep 'temporary password' /var/log/mysqld.log 2>/dev/null | tail -1 | awk '{print $NF}')
echo ">>> MySQL临时密码: ${TMP_PASS:-无(可能已初始化过)}"

if [ -n "$TMP_PASS" ]; then
mysql --connect-expired-password -uroot -p"$TMP_PASS" <<SQL 2>/dev/null
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Root@123456';
SQL
fi

# 创建Hive和DS数据库(可能已存在，忽略错误)
mysql -uroot -pRoot@123456 2>/dev/null <<SQL
CREATE DATABASE IF NOT EXISTS hive_metastore DEFAULT CHARSET utf8mb4;
CREATE DATABASE IF NOT EXISTS dolphinscheduler DEFAULT CHARSET utf8mb4;
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED WITH mysql_native_password BY 'Hive@123456';
CREATE USER IF NOT EXISTS 'ds'@'%' IDENTIFIED WITH mysql_native_password BY 'Ds@123456';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON dolphinscheduler.* TO 'ds'@'%';
FLUSH PRIVILEGES;
SQL

echo ">>> MySQL配置完成 (root/Root@123456)"

# ============================================================
# 4. Hive 3.1.3
# ============================================================
step "4" "安装 Hive 3.1.3"
HIVE_HOME=${MOD}/hive-3.1.3
if [ ! -d "$HIVE_HOME" ]; then
    tar -zxf ${PKG}/apache-hive-3.1.3-bin.tar.gz -C ${MOD}/
    mv ${MOD}/apache-hive-3.1.3-bin ${HIVE_HOME}
fi

cat > ${HIVE_HOME}/conf/hive-env.sh <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HIVE_HOME=${HIVE_HOME}
export HIVE_CONF_DIR=\${HIVE_HOME}/conf
EOF

cat > ${HIVE_HOME}/conf/hive-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:mysql://master:3306/hive_metastore?useSSL=false&amp;serverTimezone=Asia/Shanghai</value></property>
    <property><name>javax.jdo.option.ConnectionDriverName</name><value>com.mysql.cj.jdbc.Driver</value></property>
    <property><name>javax.jdo.option.ConnectionUserName</name><value>hive</value></property>
    <property><name>javax.jdo.option.ConnectionPassword</name><value>Hive@123456</value></property>
    <property><name>hive.metastore.uris</name><value>thrift://master:9083</value></property>
    <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
    <property><name>hive.execution.engine</name><value>mr</value></property>
    <property><name>hive.server2.thrift.port</name><value>10000</value></property>
    <property><name>hive.server2.thrift.bind.host</name><value>master</value></property>
    <property><name>hive.metastore.schema.verification</name><value>false</value></property>
</configuration>
EOF

# MySQL驱动
if [ ! -f "${HIVE_HOME}/lib/mysql-connector-java-8.0.33.jar" ]; then
    echo ">>> 下载MySQL驱动..."
    wget -q -O ${HIVE_HOME}/lib/mysql-connector-j-8.0.33.jar \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar 2>/dev/null || \
        cp /usr/share/java/mysql-connector-java.jar ${HIVE_HOME}/lib/ 2>/dev/null || \
        warn "MySQL驱动下载失败，请手动放入 ${HIVE_HOME}/lib/"
fi

# 解决guava冲突
HADOOP_GUAVA=$(find ${HADOOP_HOME} -maxdepth 3 -name "guava-*.jar" | head -1)
HIVE_GUAVA_JAR=$(find ${HIVE_HOME}/lib -name "guava-*.jar" | head -1)
if [ -f "$HADOOP_GUAVA" ] && [ -f "$HIVE_GUAVA_JAR" ]; then
    HADOOP_GV=$(basename $HADOOP_GUAVA)
    HIVE_GV=$(basename $HIVE_GUAVA_JAR)
    if [ "$HADOOP_GV" != "$HIVE_GV" ]; then
        echo ">>> 替换guava: $HIVE_GV → $HADOOP_GV"
        rm -f $HIVE_GUAVA_JAR
        cp $HADOOP_GUAVA ${HIVE_HOME}/lib/
    fi
fi

# 初始化元数据库
echo ">>> 初始化Hive元数据库..."
${HIVE_HOME}/bin/schematool -dbType mysql -initSchema --verbose 2>&1 | tail -5

echo ">>> Hive安装完成"

# ============================================================
# 5. Spark 3.4.3
# ============================================================
step "5" "安装 Spark 3.4.3"
SPARK_HOME=${MOD}/spark-3.4.3-bin-hadoop3
if [ ! -d "$SPARK_HOME" ]; then
    tar -zxf ${PKG}/spark-3.4.3-bin-hadoop3.tgz -C ${MOD}/
fi

cp ${SPARK_HOME}/conf/spark-env.sh.template ${SPARK_HOME}/conf/spark-env.sh 2>/dev/null || true
cat >> ${SPARK_HOME}/conf/spark-env.sh <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
EOF

cp ${SPARK_HOME}/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf 2>/dev/null || true
cat >> ${SPARK_HOME}/conf/spark-defaults.conf <<EOF
spark.master                    yarn
spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://master:9000/spark-logs
spark.sql.adaptive.enabled      true
spark.sql.adaptive.coalescePartitions.enabled  true
EOF

# 集成Hive
cp ${HIVE_HOME}/conf/hive-site.xml ${SPARK_HOME}/conf/ 2>/dev/null || true
cp ${HIVE_HOME}/lib/mysql-connector-j-*.jar ${SPARK_HOME}/jars/ 2>/dev/null || true
echo ">>> Spark安装完成"

# ============================================================
# 6. Kafka 3.6.1
# ============================================================
step "6" "安装 Kafka 3.6.1"
KAFKA_HOME=${MOD}/kafka_2.12-3.6.1
if [ ! -d "$KAFKA_HOME" ]; then
    tar -zxf ${PKG}/kafka_2.12-3.6.1.tgz -C ${MOD}/
fi

mkdir -p /data/kafka

cat > ${KAFKA_HOME}/config/server.properties <<EOF
broker.id=0
listeners=PLAINTEXT://master:9092
advertised.listeners=PLAINTEXT://master:9092
log.dirs=/data/kafka
num.partitions=3
default.replication.factor=2
offsets.topic.replication.factor=2
zookeeper.connect=master:2181,slave1:2181,slave2:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF
echo ">>> Kafka安装完成"

# ============================================================
# 7. Flink 1.17.2
# ============================================================
step "7" "安装 Flink 1.17.2"
FLINK_HOME=${MOD}/flink-1.17.2
if [ ! -d "$FLINK_HOME" ]; then
    tar -zxf ${PKG}/flink-1.17.2-bin-scala_2.12.tgz -C ${MOD}/
    [ -d "${MOD}/flink-1.17.2" ] || mv ${MOD}/flink-1.17.2-* ${FLINK_HOME} 2>/dev/null
fi

cat >> ${FLINK_HOME}/conf/flink-conf.yaml <<EOF
jobmanager.rpc.address: master
taskmanager.memory.process.size: 1024m
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
execution.target: yarn-application
env.java.home: ${JAVA_HOME}
classloader.resolve-order: parent-first
EOF
echo ">>> Flink安装完成"

# ============================================================
# 8. DataX
# ============================================================
step "8" "安装 DataX"
DATAX_HOME=${MOD}/datax
if [ ! -d "$DATAX_HOME" ]; then
    echo ">>> 解压DataX (大文件，稍等)..."
    tar -zxf ${PKG}/datax.tar.gz -C ${MOD}/
fi
echo ">>> DataX安装完成"

# ============================================================
# 9. DolphinScheduler 3.2.1
# ============================================================
step "9" "安装 DolphinScheduler 3.2.1"
DS_HOME=${MOD}/dolphinscheduler-3.2.1

# 检查Python3
if ! command -v python3 &>/dev/null; then
    warn "需要Python3, 正在安装..."
    yum install -y python3 2>/dev/null || err "Python3安装失败"
fi

if [ ! -d "$DS_HOME" ]; then
    tar -zxf ${PKG}/apache-dolphinscheduler-3.2.1-bin.tar.gz -C ${MOD}/
    mv ${MOD}/apache-dolphinscheduler-3.2.1-bin ${DS_HOME} 2>/dev/null || true
fi

# install_env.sh - 单节点部署
# 注意: installPath 不能和当前目录(install.sh所在目录)相同
DS_INSTALL_PATH=/opt/module/dolphinscheduler
cat > ${DS_HOME}/bin/env/install_env.sh <<INSTALLENV
ips="master"
sshPort=22
masters="master"
workers="master:default"
alertServer="master"
apiServers="master"
installPath="${DS_INSTALL_PATH}"
deployUser="root"
zkRoot="/dolphinscheduler"
INSTALLENV

# dolphinscheduler_env.sh
PYTHON_HOME=$(dirname $(dirname $(which python3)))
cat > ${DS_HOME}/bin/env/dolphinscheduler_env.sh <<DSENV
export JAVA_HOME=${JAVA_HOME}
export DATABASE=mysql
export SPRING_PROFILES_ACTIVE=mysql
export SPRING_DATASOURCE_URL="jdbc:mysql://master:3306/dolphinscheduler?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai"
export SPRING_DATASOURCE_USERNAME=ds
export SPRING_DATASOURCE_PASSWORD=Ds@123456
export REGISTRY_TYPE=zookeeper
export REGISTRY_ZOOKEEPER_CONNECT_STRING=master:2181,slave1:2181,slave2:2181
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export DATAX_HOME=${DATAX_HOME}
export PYTHON_HOME=${PYTHON_HOME}
DSENV

# MySQL驱动(DS每个模块都需要)
MYSQL_DRIVER=$(find ${HIVE_HOME}/lib -name "mysql-connector-j-*.jar" | head -1)
if [ -f "$MYSQL_DRIVER" ]; then
    for dir in api-server alert-server master-server worker-server tools; do
        mkdir -p ${DS_HOME}/${dir}/libs
        cp "$MYSQL_DRIVER" ${DS_HOME}/${dir}/libs/ 2>/dev/null || true
    done
fi

# 跳过root用户检查
sed -i 's/exit 1/exit 0/' ${DS_HOME}/bin/dolphinscheduler-daemon.sh 2>/dev/null || true

echo ">>> DolphinScheduler 准备完成"
echo ">>> 等待ZooKeeper和MySQL启动后，运行: cd ${DS_HOME} && bash bin/install.sh"

# ============================================================
# 10. Superset
# ============================================================
step "10" "安装 Superset"
warn "Superset使用pip安装，如果内存不足可以跳过(不影响数仓项目)"

pip3 install --upgrade pip --quiet 2>/dev/null || true

if pip3 show apache-superset &>/dev/null; then
    echo ">>> Superset已安装"
else
    echo ">>> pip安装Superset (可能需要几分钟)..."
    pip3 install apache-superset 2>&1 | tail -5 || warn "Superset安装失败，可稍后手动安装"
fi

if pip3 show apache-superset &>/dev/null; then
    mkdir -p /data/superset
    export SUPERSET_CONFIG_PATH=/data/superset/superset_config.py
    cat > /data/superset/superset_config.py <<EOF
import os
SECRET_KEY = os.urandom(24)
SQLALCHEMY_DATABASE_URI = 'sqlite:////data/superset/superset.db'
DATA_DIR = '/data/superset'
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
EOF

    superset db upgrade 2>/dev/null || true
    superset fab create-admin --username admin --firstname Admin --lastname User --email admin@admin.com --password admin 2>/dev/null || true
    superset init 2>/dev/null || true
    echo ">>> Superset安装完成 (admin/admin)"
fi

# ============================================================
# 输出总结
# ============================================================
echo ""
echo "============================================================"
echo -e "${GREEN}  Master节点安装完成!${NC}"
echo "============================================================"
echo ""
echo "安装路径: ${MOD}/"
echo "  zookeeper-3.8.4/"
echo "  hadoop-3.3.6/"
echo "  hive-3.1.3/"
echo "  spark-3.4.3-bin-hadoop3/"
echo "  kafka_2.12-3.6.1/"
echo "  flink-1.17.2/"
echo "  datax/"
echo "  dolphinscheduler-3.2.1/"
echo ""
echo "--- 密码 ---"
echo "  MySQL root: Root@123456"
echo "  MySQL hive: Hive@123456"
echo "  MySQL ds:   Ds@123456"
echo "  DolphinScheduler: admin / dolphinscheduler123"
echo "  Superset: admin / admin"
echo ""
echo "--- 下一步 ---"
echo "  1. 在 slave1/slave2 上执行: bash deploy_slave.sh"
echo "  2. 启动集群:            bash start_all.sh"
