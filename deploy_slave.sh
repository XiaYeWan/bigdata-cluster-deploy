#!/bin/bash
#===========================================================
# deploy_slave.sh - Slave节点部署 (slave1 / slave2)
# 包含: ZooKeeper, HDFS(DataNode), YARN(NodeManager), Kafka
# 注意: 必须先在master上把压缩包scp过来再执行
#===========================================================
set -e

GREEN='\033[32m'; RED='\033[31m'; NC='\033[0m'
PKG=/root
MOD=/opt/module
THIS_HOST=$(hostname)

# 动态判断myid和broker_id
case $THIS_HOST in
    master) MYID=1; BROKER_ID=0 ;;
    slave1) MYID=2; BROKER_ID=1 ;;
    slave2) MYID=3; BROKER_ID=2 ;;
    *) echo "ERROR: hostname必须是 master/slave1/slave2"; exit 1 ;;
esac

# JAVA_HOME
JAVA_BIN=$(which java)
JAVA_HOME=$(readlink -f $JAVA_BIN | sed 's|/jre/bin/java||' | sed 's|/bin/java||')
echo -e "${GREEN}${THIS_HOST}: myid=${MYID}, broker.id=${BROKER_ID}, JAVA_HOME=${JAVA_HOME}${NC}"

mkdir -p ${MOD} /data/hdfs/{namenode,datanode} /data/zookeeper /data/kafka

echo ""
echo "============================================"
echo "  ${THIS_HOST} 开始安装"
echo "============================================"

# ============================================================
# 1. ZooKeeper
# ============================================================
echo "[1/3] 安装 ZooKeeper..."
ZK_HOME=${MOD}/zookeeper-3.8.4
if [ ! -d "$ZK_HOME" ]; then
    tar -zxf ${PKG}/apache-zookeeper-3.8.4-bin.tar.gz -C ${MOD}/ || {
        echo ">>> 压缩包不存在，从master同步..."
        scp master:/root/apache-zookeeper-3.8.4-bin.tar.gz ${PKG}/
        tar -zxf ${PKG}/apache-zookeeper-3.8.4-bin.tar.gz -C ${MOD}/
    }
    mv ${MOD}/apache-zookeeper-3.8.4-bin ${ZK_HOME}
fi

echo ${MYID} > /data/zookeeper/myid

cat > ${ZK_HOME}/conf/zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data/zookeeper
clientPort=2181
server.1=master:2888:3888
server.2=slave1:2888:3888
server.3=slave2:2888:3888
EOF
echo "  ZooKeeper OK (myid=${MYID})"

# ============================================================
# 2. Hadoop (DataNode + NodeManager)
# ============================================================
echo "[2/3] 安装 Hadoop..."
HADOOP_HOME=${MOD}/hadoop-3.3.6
if [ ! -d "$HADOOP_HOME" ]; then
    tar -zxf ${PKG}/hadoop-3.3.6.tar.gz -C ${MOD}/ || {
        scp master:/root/hadoop-3.3.6.tar.gz ${PKG}/
        tar -zxf ${PKG}/hadoop-3.3.6.tar.gz -C ${MOD}/
    }
fi

# 配置文件从master拉取
echo "  从master同步Hadoop配置..."
scp -r master:${HADOOP_HOME}/etc/hadoop/* ${HADOOP_HOME}/etc/hadoop/ 2>/dev/null || {
    # 如果拉取失败，手动写配置
    cat >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh <<EOF
export JAVA_HOME=${JAVA_HOME}
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export YARN_NODEMANAGER_USER=root
export HADOOP_LOG_DIR=/data/hadoop/logs
EOF
    mkdir -p /data/hadoop/logs /data/hadoop/tmp
}
echo "  Hadoop OK"

# ============================================================
# 3. Kafka
# ============================================================
echo "[3/3] 安装 Kafka..."
KAFKA_HOME=${MOD}/kafka_2.12-3.6.1
if [ ! -d "$KAFKA_HOME" ]; then
    tar -zxf ${PKG}/kafka_2.12-3.6.1.tgz -C ${MOD}/ || {
        scp master:/root/kafka_2.12-3.6.1.tgz ${PKG}/
        tar -zxf ${PKG}/kafka_2.12-3.6.1.tgz -C ${MOD}/
    }
fi

cat > ${KAFKA_HOME}/config/server.properties <<EOF
broker.id=${BROKER_ID}
listeners=PLAINTEXT://${THIS_HOST}:9092
advertised.listeners=PLAINTEXT://${THIS_HOST}:9092
log.dirs=/data/kafka
num.partitions=3
default.replication.factor=2
offsets.topic.replication.factor=2
zookeeper.connect=master:2181,slave1:2181,slave2:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF
echo "  Kafka OK (broker.id=${BROKER_ID})"

echo ""
echo "============================================"
echo -e "${GREEN}  ${THIS_HOST} 安装完成!${NC}"
echo "============================================"
echo "  等待master执行 start_all.sh 统一启动集群"
