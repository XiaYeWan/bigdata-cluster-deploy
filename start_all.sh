#!/bin/bash
#===========================================================
# start_all.sh - 启动全部集群服务 (在master执行)
#===========================================================

MOD=/opt/module
SUPERSET_PORT=9088
RED='\033[31m'; GREEN='\033[32m'; NC='\033[0m'

echo "============================================"
echo "  启动大数据集群"
echo "============================================"

# 1. ZooKeeper (3台)
echo "[1/9] 启动ZooKeeper..."
for host in master slave1 slave2; do
    ssh $host "${MOD}/zookeeper-3.8.4/bin/zkServer.sh start" 2>/dev/null
done
sleep 8
echo "  ZooKeeper状态:"
for host in master slave1 slave2; do
    echo -n "    $host: "
    for i in 1 2 3; do
        mode=$(ssh $host "${MOD}/zookeeper-3.8.4/bin/zkServer.sh status" 2>/dev/null | grep -o 'Mode:.*')
        [ -n "$mode" ] && echo "$mode" && break
        [ "$i" -lt 3 ] && sleep 3
    done
    [ -z "$mode" ] && echo "选举中(稍后自动完成)"
done

# 2. HDFS
echo "[2/9] 启动HDFS..."
${MOD}/hadoop-3.3.6/sbin/start-dfs.sh 2>&1 | grep -v "上一次登录" | grep -v "ERROR: Cannot set priority"
sleep 5

# 3. YARN
echo "[3/9] 启动YARN..."
${MOD}/hadoop-3.3.6/sbin/start-yarn.sh 2>&1 | grep -v "上一次登录"
sleep 3

# 4. 创建HDFS目录
echo "[4/9] 创建HDFS工作目录..."
${MOD}/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /spark-logs 2>/dev/null || true
${MOD}/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /user/hive/warehouse 2>/dev/null || true
echo "  HDFS目录OK"

# 5. Hive Metastore + HiveServer2
echo "[5/9] 启动Hive..."
nohup ${MOD}/hive-3.1.3/bin/hive --service metastore > /dev/null 2>&1 &
sleep 3
nohup ${MOD}/hive-3.1.3/bin/hive --service hiveserver2 > /dev/null 2>&1 &
sleep 3
echo "  Hive Metastore(9083) + HiveServer2(10000)"

# 6. Spark HistoryServer
echo "[6/9] 启动Spark HistoryServer..."
mkdir -p /tmp/spark-events
${MOD}/spark-3.4.3-bin-hadoop3/sbin/start-history-server.sh 2>/dev/null
echo "  Spark HistoryServer: http://master:18080"

# 7. Kafka (3台)
echo "[7/9] 启动Kafka..."
for host in master slave1 slave2; do
    echo -n "  Kafka $host..."
    ssh $host "nohup ${MOD}/kafka_2.12-3.6.1/bin/kafka-server-start.sh -daemon ${MOD}/kafka_2.12-3.6.1/config/server.properties > /dev/null 2>&1 &" 2>/dev/null
    echo "done"
    sleep 2
done
sleep 3
echo "  Kafka各节点已启动"

# 8. DolphinScheduler
echo "[8/9] 启动DolphinScheduler..."
DS_HOME=/opt/module/dolphinscheduler
if [ -f "${DS_HOME}/bin/dolphinscheduler-daemon.sh" ]; then
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh start master-server 2>/dev/null
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh start worker-server 2>/dev/null
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh start alert-server 2>/dev/null
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh start api-server 2>/dev/null
    sleep 3
    echo "  DS: http://master:12345/dolphinscheduler (admin/dolphinscheduler123)"
else
    echo "  DolphinScheduler未安装，跳过"
fi

# 9. Superset (可选)
echo "[9/9] Superset (可选)..."
if command -v superset &>/dev/null; then
    mkdir -p /data/superset
    nohup superset run -h 0.0.0.0 -p ${SUPERSET_PORT} --with-threads > /data/superset/superset.log 2>&1 &
    echo "  Superset: http://master:${SUPERSET_PORT} (admin/admin)"
else
    echo "  Superset未安装，跳过"
fi

echo ""
echo "============================================"
echo -e "  ${GREEN}集群启动完成!${NC}"
echo "============================================"
echo ""

# 进程检查 - 用ps aux避免jps路径问题
echo "--- 各节点Java进程 ---"
for host in master slave1 slave2; do
    echo -n "[$host] "
    ssh $host "ps aux 2>/dev/null | grep java | grep -v grep | awk -F'/' '{for(i=1;i<=NF;i++){if(\$i ~ /org\.apache|kafka\.Kafka|QuorumPeer|NameNode|DataNode|ResourceManager|NodeManager|Hive|HistoryServer|dolphinscheduler/){printf \"%s \", \$i}}}'" 2>/dev/null | head -10
    echo ""
done

echo ""
echo "--- Web 控制台 ---"
echo "  HDFS:              http://master:9870"
echo "  YARN:              http://master:8088"
echo "  Spark History:     http://master:18080"
echo "  DolphinScheduler:  http://master:12345/dolphinscheduler (admin/dolphinscheduler123)"
echo "  Superset:          http://master:${SUPERSET_PORT} (admin/admin)"
echo ""
