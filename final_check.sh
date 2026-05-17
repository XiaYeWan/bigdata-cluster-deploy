#!/bin/bash
# ============================================================
# 一键最终验证脚本
# ============================================================
MOD=/opt/module

echo "============================================"
echo "  大数据集群最终验证"
echo "  $(date)"
echo "============================================"

echo ""
echo ">>> ZooKeeper"
for h in master slave1 slave2; do
    c=$(ssh $h "ps aux | grep QuorumPeer | grep -v grep | wc -l" 2>/dev/null)
    [ "$c" -ge 1 ] && echo "  ✅ $h" || echo "  ❌ $h"
done

echo ""
echo ">>> HDFS"
ssh master "ps aux | grep proc_namenode | grep -v grep | wc -l" | xargs -I{} [ {} -ge 1 ] && echo "  ✅ NameNode" || echo "  ❌ NameNode"
for h in master slave1 slave2; do
    c=$(ssh $h "ps aux | grep proc_datanode | grep -v grep | wc -l" 2>/dev/null)
    [ "$c" -ge 1 ] && echo "  ✅ DataNode($h)" || echo "  ❌ DataNode($h)"
done
/opt/module/hadoop-3.3.6/bin/hdfs dfs -ls / >/dev/null 2>&1 && echo "  ✅ HDFS 读写" || echo "  ❌ HDFS 读写"

echo ""
echo ">>> YARN"
ssh master "ps aux | grep proc_resourcemanager | grep -v grep | wc -l" | xargs -I{} [ {} -ge 1 ] && echo "  ✅ ResourceManager" || echo "  ❌ RM"
for h in master slave1 slave2; do
    c=$(ssh $h "ps aux | grep proc_nodemanager | grep -v grep | wc -l" 2>/dev/null)
    [ "$c" -ge 1 ] && echo "  ✅ NodeManager($h)" || echo "  ❌ NodeManager($h)"
done

echo ""
echo ">>> MySQL"
ssh master "mysql -uroot -pRoot@123456 -e 'SELECT 1' 2>/dev/null | grep -c 1" | xargs -I{} [ {} -ge 1 ] && echo "  ✅ MySQL" || echo "  ❌ MySQL"

echo ""
echo ">>> Hive"
ssh master "ps aux | grep HiveMetaStore | grep -v grep | wc -l" | xargs -I{} [ {} -ge 1 ] && echo "  ✅ Metastore(9083)" || echo "  ❌ Metastore"
ssh master "/opt/module/hive-3.1.3/bin/hive -e 'show databases;' 2>/dev/null | grep -c default" | xargs -I{} [ {} -ge 1 ] && echo "  ✅ Hive CLI" || echo "  ❌ Hive CLI"

echo ""
echo ">>> Spark"
/opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit --version 2>&1 | grep -o 'version [0-9.]*' | head -1 && echo "  ✅ Spark OK" || echo "  ❌ Spark"

echo ""
echo ">>> Kafka"
k=$(for h in master slave1 slave2; do ssh $h "ps aux | grep kafka.Kafka | grep -v grep | wc -l" 2>/dev/null; done | paste -sd+ | bc)
[ "$k" -ge 3 ] && echo "  ✅ Kafka 3进程" || echo "  ❌ Kafka($k/3)"
/opt/module/kafka_2.12-3.6.1/bin/kafka-topics.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 --list >/dev/null 2>&1 && echo "  ✅ Kafka broker" || echo "  ❌ Kafka broker"

echo ""
echo ">>> DataX"
/opt/module/datax/bin/datax.py 2>&1 | grep -qi 'usage\|DataX' && echo "  ✅ DataX" || echo "  ❌ DataX"

echo ""
echo "============================================"
echo "  验证完成"
echo "============================================"
