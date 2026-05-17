#!/bin/bash
#===========================================================
# stop_all.sh - 停止全部集群服务 (在master执行)
# 停止顺序: 上层 → 底层 (ZK最后停)
#===========================================================

MOD=/opt/module

echo "============================================"
echo "  停止大数据集群"
echo "============================================"

# 1. Superset
echo "[1/7] 停止Superset..."
pkill -f "superset run" 2>/dev/null || true

# 2. DolphinScheduler
echo "[2/7] 停止DolphinScheduler..."
DS_HOME=/opt/module/dolphinscheduler
if [ -f "${DS_HOME}/bin/dolphinscheduler-daemon.sh" ]; then
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh stop api-server 2>/dev/null || true
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh stop alert-server 2>/dev/null || true
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh stop worker-server 2>/dev/null || true
    ${DS_HOME}/bin/dolphinscheduler-daemon.sh stop master-server 2>/dev/null || true
    sleep 2
    echo "  DS已停止"
else
    echo "  DS未安装，跳过"
fi

# 3. Hive
echo "[3/7] 停止Hive..."
pkill -f HiveMetaStore 2>/dev/null || true
pkill -f HiveServer2 2>/dev/null || true

# 4. Spark HistoryServer
echo "[4/7] 停止Spark HistoryServer..."
${MOD}/spark-3.4.3-bin-hadoop3/sbin/stop-history-server.sh 2>/dev/null || true

# 5. Kafka
echo "[5/7] 停止Kafka..."
for host in master slave1 slave2; do
    echo -n "  Kafka $host..."
    timeout 10 ssh $host "${MOD}/kafka_2.12-3.6.1/bin/kafka-server-stop.sh" 2>/dev/null || \
        ssh $host "pkill -f 'kafka.Kafka' 2>/dev/null" || true
    echo "done"
done
sleep 3

# 6. YARN
echo "[6/7] 停止YARN..."
${MOD}/hadoop-3.3.6/sbin/stop-yarn.sh 2>/dev/null | grep -v "上一次登录" || true

# 7. HDFS
echo "[7/7] 停止HDFS..."
${MOD}/hadoop-3.3.6/sbin/stop-dfs.sh 2>/dev/null | grep -v "上一次登录" || true

# 最后停ZK
echo ""
echo "停止ZooKeeper..."
for host in master slave1 slave2; do
    ssh $host "${MOD}/zookeeper-3.8.4/bin/zkServer.sh stop" 2>/dev/null || true
done

sleep 2
echo ""
echo "============================================"
echo "  集群已停止"
echo "============================================"
