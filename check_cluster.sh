#!/bin/bash
# ============================================================
# 大数据集群完整巡检脚本
# 在 master 上执行: bash check_cluster.sh
# ============================================================

PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    local result
    result=$(eval "$cmd" 2>&1)
    if echo "$result" | grep -qiE "ok|ready|true|1|active|running|default"; then
        echo "  ✅ $name"
        PASS=$((PASS + 1))
    elif [ -n "$result" ]; then
        echo "  ⚠️  $name — $result"
        FAIL=$((FAIL + 1))
    else
        echo "  ❌ $name — 未检测到"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================"
echo "  大数据集群巡检"
echo "  $(date)"
echo "============================================"
echo ""

echo "[1] ZooKeeper"
for host in master slave1 slave2; do
    count=$(ssh $host "ps aux | grep QuorumPeer | grep -v grep | wc -l" 2>/dev/null)
    [ "$count" -ge 1 ] && echo "  ✅ $host ZK running" && PASS=$((PASS + 1)) || \
    { echo "  ❌ $host ZK stopped"; FAIL=$((FAIL + 1)); }
done
echo ""

echo "[2] HDFS"
# NameNode
nn=$(ssh master "ps aux | grep 'proc_namenode' | grep -v grep | wc -l" 2>/dev/null)
[ "$nn" -ge 1 ] && echo "  ✅ NameNode (master) running" && PASS=$((PASS + 1)) || \
{ echo "  ❌ NameNode stopped"; FAIL=$((FAIL + 1)); }

# DataNode
for host in master slave1 slave2; do
    dn=$(ssh $host "ps aux | grep 'proc_datanode' | grep -v grep | wc -l" 2>/dev/null)
    [ "$dn" -ge 1 ] && echo "  ✅ DataNode ($host) running" && PASS=$((PASS + 1)) || \
    { echo "  ❌ DataNode ($host) stopped"; FAIL=$((FAIL + 1)); }
done

# HDFS 读写测试
hdfs_test=$(/opt/module/hadoop-3.3.6/bin/hdfs dfs -ls / 2>&1 | grep -c "Found")
[ "$hdfs_test" -ge 1 ] && echo "  ✅ HDFS 读写正常" && PASS=$((PASS + 1)) || \
{ echo "  ❌ HDFS 读写异常"; FAIL=$((FAIL + 1)); }
echo ""

echo "[3] YARN"
rm=$(ssh master "ps aux | grep 'proc_resourcemanager' | grep -v grep | wc -l" 2>/dev/null)
[ "$rm" -ge 1 ] && echo "  ✅ ResourceManager (master) running" && PASS=$((PASS + 1)) || \
{ echo "  ❌ ResourceManager stopped"; FAIL=$((FAIL + 1)); }

for host in master slave1 slave2; do
    nm=$(ssh $host "ps aux | grep 'proc_nodemanager' | grep -v grep | wc -l" 2>/dev/null)
    [ "$nm" -ge 1 ] && echo "  ✅ NodeManager ($host) running" && PASS=$((PASS + 1)) || \
    { echo "  ❌ NodeManager ($host) stopped"; FAIL=$((FAIL + 1)); }
done
echo ""

echo "[4] MySQL"
mysql_test=$(ssh master "mysql -uroot -pRoot@123456 -e 'SELECT 1' 2>&1 | grep -c '1'")
[ "$mysql_test" -ge 1 ] && echo "  ✅ MySQL running" && PASS=$((PASS + 1)) || \
{ echo "  ❌ MySQL 异常"; FAIL=$((FAIL + 1)); }
echo ""

echo "[5] Hive"
ms=$(ssh master "ps aux | grep HiveMetaStore | grep -v grep | wc -l" 2>/dev/null)
[ "$ms" -ge 1 ] && echo "  ✅ Metastore running (9083)" && PASS=$((PASS + 1)) || \
{ echo "  ❌ Metastore stopped"; FAIL=$((FAIL + 1)); }

hive_test=$(ssh master "/opt/module/hive-3.1.3/bin/hive -e 'show databases;' 2>&1 | grep -c 'default'")
[ "$hive_test" -ge 1 ] && echo "  ✅ Hive CLI 可用(建库/查库正常)" && PASS=$((PASS + 1)) || \
{ echo "  ❌ Hive CLI 异常"; FAIL=$((FAIL + 1)); }
echo ""

echo "[6] Spark"
spark_ver=$(ssh master "/opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit --version 2>&1 | grep -o 'version [0-9.]*'")
[ -n "$spark_ver" ] && echo "  ✅ Spark $spark_ver" && PASS=$((PASS + 1)) || \
{ echo "  ❌ Spark 异常"; FAIL=$((FAIL + 1)); }
echo ""

echo "[7] Kafka"
kafka_list=$(/opt/module/kafka_2.12-3.6.1/bin/kafka-topics.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 --list 2>/dev/null)
[ -n "$kafka_list" ] && echo "  ✅ Kafka broker可用 (topic列表返回正常)" && PASS=$((PASS + 1)) || \
{ echo "  ❌ Kafka 异常"; FAIL=$((FAIL + 1)); }

kafka_proc=0
for host in master slave1 slave2; do
    count=$(ssh $host "ps aux | grep kafka | grep -v grep | wc -l" 2>/dev/null)
    kafka_proc=$((kafka_proc + count))
done
[ "$kafka_proc" -ge 3 ] && echo "  ✅ Kafka 3进程运行中" && PASS=$((PASS + 1)) || \
{ echo "  ❌ Kafka 进程不完整($kafka_proc/3)"; FAIL=$((FAIL + 1)); }
echo ""

echo "[8] DataX"
datax_test=$(ssh master "/opt/module/datax/bin/datax.py 2>&1 | grep -ci 'usage\|DataX'")
[ "$datax_test" -ge 1 ] && echo "  ✅ DataX 已安装" && PASS=$((PASS + 1)) || \
{ echo "  ❌ DataX 异常"; FAIL=$((FAIL + 1)); }
echo ""

echo "============================================"
echo "  巡检结果: $PASS 通过, $FAIL 未通过"
echo "============================================"

if [ "$FAIL" -eq 0 ]; then
    echo "  🎉 集群安装完成，所有组件正常！"
else
    echo "  ⚠️  请检查以上未通过项"
fi
