# 🏗️ Big Data Cluster — 从零搭建离线数仓全栈集群

<p align="center">
  <img src="https://img.shields.io/badge/CentOS-7-blue?logo=centos" alt="CentOS">
  <img src="https://img.shields.io/badge/Java-1.8-orange?logo=openjdk" alt="Java">
  <img src="https://img.shields.io/badge/Hadoop-3.3.6-yellow?logo=apachehadoop" alt="Hadoop">
  <img src="https://img.shields.io/badge/Spark-3.4.3-red?logo=apachespark" alt="Spark">
  <img src="https://img.shields.io/badge/Flink-1.17.2-ff69b4?logo=apacheflink" alt="Flink">
  <img src="https://img.shields.io/badge/Kafka-3.6.1-white?logo=apachekafka" alt="Kafka">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/status-active-success" alt="Status">
</p>

> 🎯 3 台 CentOS 7 虚拟机，一键部署 11 个大数据组件，覆盖完整离线数仓技术栈。  
> 🔧 附 10 个真实排错案例 + 6 个 Shell 自动化脚本。  
> 📝 **声明**：本项目为个人学习实践，所有组件运行于 VMware 虚拟机环境，非生产部署。

---

## 📖 目录

- [集群概况](#-集群概况)
- [环境要求](#-环境要求)
- [一键部署](#-一键部署)
- [组件清单](#-组件清单--版本矩阵)
- [Web 控制台](#-web-控制台)
- [账号速查](#-账号速查)
- [脚本说明](#-脚本说明)
- [排错实战](#-排错实战-10-个问题--全部解决)
- [下一步计划](#-下一步计划)
- [License](#-license)

---

## 📋 集群概况

| 节点 | 角色 | 建议内存 | 核心组件 |
|------|------|----------|----------|
| **master** | 主节点 | 6-8G | NameNode, ResourceManager, ZK, MySQL, Hive, Spark, Kafka, DataX, DolphinScheduler, Superset |
| **slave1** | 从节点 | 4-6G | DataNode, NodeManager, ZK, Kafka |
| **slave2** | 从节点 | 4-6G | DataNode, NodeManager, ZK, Kafka |

```
┌─────────────────────────────────────────────────────────┐
│                    可视化 & 调度                         │
│  Superset(报表)  DolphinScheduler(任务编排)              │
├─────────────────────────────────────────────────────────┤
│                    数据链路层                            │
│  DataX(Sync) → Hive(ODS→DWD→DWS→ADS) → Spark(计算)     │
│  Kafka(实时采集) → Flink(流处理 → HDFS)                  │
├─────────────────────────────────────────────────────────┤
│                    存储 & 资源层                         │
│  HDFS(分布式存储)  YARN(资源调度)  MySQL(元数据)          │
├─────────────────────────────────────────────────────────┤
│                    协调 & 基础层                         │
│  ZooKeeper(集群协调)  JDK 1.8  CentOS 7                 │
└─────────────────────────────────────────────────────────┘
```

---

## 🔧 环境要求

| 项目 | 要求 |
|------|------|
| 操作系统 | CentOS 7（已 EOL，脚本内置 vault 源切换） |
| JDK | OpenJDK 1.8.0 |
| 节点数 | 3 台（master, slave1, slave2） |
| SSH | master → 所有节点免密登录 |
| 宿主机浏览器 | Chrome ≥ 85 / Edge ≥ 85（访问 Web UI） |

### 前置步骤

```bash
# 1. 配置 hosts（所有节点）
cat >> /etc/hosts <<EOF
192.168.254.129 master
192.168.254.128 slave1
192.168.254.130 slave2
EOF

# 2. SSH 免密（仅 master 执行）
ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
for host in master slave1 slave2; do
    ssh-copy-id $host
done

# 3. 关闭防火墙（所有节点）
for host in master slave1 slave2; do
    ssh $host "systemctl stop firewalld && systemctl disable firewalld"
done

# 4. 安装包下载到 /root/（所有节点，详见下方组件清单）
# 5. 克隆本仓库到 master: git clone https://github.com/xxx/bigdata-deploy.git /root/bigdata-deploy
```

---

## 🚀 一键部署

```bash
# Step 1: Master 节点部署（11 个组件）
cd /root/bigdata-deploy
bash deploy_master.sh

# Step 2: Slave 节点部署（2 个节点并行）
ssh slave1 "mkdir -p /root/bigdata-deploy && cd /root/bigdata-deploy && bash -s" < deploy_slave.sh &
ssh slave2 "mkdir -p /root/bigdata-deploy && cd /root/bigdata-deploy && bash -s" < deploy_slave.sh &
wait

# Step 3: 统一启动
bash start_all.sh

# Step 4: 健康检查
bash check_cluster.sh
```

## 📦 组件清单 & 版本矩阵

| 组件 | 版本 | 部署方式 | 安装路径 | 下载地址 |
|------|------|----------|----------|----------|
| JDK | 1.8.0_262 | `yum install` | `/usr/lib/jvm/java-1.8.0-openjdk-...` | 系统源 |
| ZooKeeper | 3.8.4 | tar.gz | `/opt/module/zookeeper-3.8.4` | [下载](https://archive.apache.org/dist/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz) |
| Hadoop | 3.3.6 | tar.gz | `/opt/module/hadoop-3.3.6` | [下载](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) |
| MySQL | 8.0.46 | yum | systemd | [下载](https://dev.mysql.com/downloads/repo/yum/) |
| Hive | 3.1.3 | tar.gz | `/opt/module/hive-3.1.3` | [下载](https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz) |
| Spark | 3.4.3 | tgz (YARN模式) | `/opt/module/spark-3.4.3-bin-hadoop3` | [下载](https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz) |
| Kafka | 3.6.1 | tgz | `/opt/module/kafka_2.12-3.6.1` | [下载](https://archive.apache.org/dist/kafka/3.6.1/kafka_2.12-3.6.1.tgz) |
| Flink | 1.17.2 | tgz (YARN模式) | `/opt/module/flink-1.17.2` | [下载](https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz) |
| DataX | 最新 | tar.gz | `/opt/module/datax` | [下载](https://github.com/alibaba/DataX) |
| DolphinScheduler | 3.2.1 | tar.gz | `/opt/module/dolphinscheduler` | [下载](https://dlcdn.apache.org/dolphinscheduler/3.2.1/apache-dolphinscheduler-3.2.1-bin.tar.gz) |
| Superset | 0.38.1 | pip3 | Python 3.6 | `pip3 install apache-superset` |

> ⚠️ 安装包不入 Git 仓库（已配置 `.gitignore`）。请在部署前手动下载放到各节点 `/root/`。

---

## 🌐 Web 控制台

| 服务 | 地址 | 账号 | 密码 |
|------|------|------|------|
| HDFS NameNode | http://master:9870 | — | — |
| YARN ResourceManager | http://master:8088 | — | — |
| Spark HistoryServer | http://master:18080 | — | — |
| Superset | http://master:9088 | admin | admin |
| DolphinScheduler | http://master:12345/dolphinscheduler | admin | dolphinscheduler123 |

> ⚠️ DolphinScheduler UI 需要 Chrome ≥ 85 或 Edge ≥ 85。  
> ⚠️ 外部访问前需关闭各节点防火墙（见前置步骤）。

---

## 🔑 账号速查

| 服务 | 地址 | 账号 | 密码 |
|------|------|------|------|
| MySQL root | master:3306 | root | Root@123456 |
| MySQL hive | master:3306 | hive | Hive@123456 |
| MySQL dolphinscheduler | master:3306 | ds | Ds@123456 |
| Hive Metastore | master:9083 | — | — |
| DolphinScheduler | master:12345 | admin | dolphinscheduler123 |
| Superset | master:9088 | admin | admin |

> 🔒 **安全提醒**: 以上为学习环境密码，生产环境请务必更换强密码。

---

## 🗂️ 脚本说明

| 脚本 | 用途 | 执行节点 |
|------|------|----------|
| `deploy_master.sh` | Master 节点 11 组件一键部署 | master |
| `deploy_slave.sh` | Slave 节点 ZK/Hadoop/Kafka 部署 | slave1, slave2 |
| `start_all.sh` | 按依赖顺序启动全部 12 个服务 | master |
| `stop_all.sh` | 按依赖逆序停止全部服务 | master |
| `check_cluster.sh` | 全组件健康巡检（19 项检查） | master |
| `final_check.sh` | 精简版快速验证 | master |

### 启动顺序

```
ZooKeeper(3台)
  → HDFS(NameNode + DataNode×3)
    → YARN(RM + NM×3)
      → HDFS 目录创建
        → Hive Metastore + HiveServer2
          → Spark HistoryServer
            → Kafka(3台)
              → DolphinScheduler(4进程)
                → Superset
```

---

## 🐛 排错实战 (10 个问题 → 全部解决)

### 🟥 问题 1: CentOS 7 YUM 源下线 + MySQL GPG Key 过期

**现象**:
```
Cannot find a valid baseurl for repo: base/7/x86_64
Could not retrieve mirrorlist http://mirrorlist.centos.org/
```

**根因**: CentOS 7 在 2024 年 6 月 EOL，`mirrorlist.centos.org` 下线。MySQL 8.0 旧 GPG key 同时过期。

**解决**:
```bash
# 1. 替换 yum 源为 vault 归档站
sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*.repo
sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
yum clean all && yum makecache

# 2. 导入新 GPG key
rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022

# 3. 安装
yum install -y mysql-community-server
```

> 💡 EOL 操作系统必须先修仓库源。

---

### 🟥 问题 2: MySQL 8.0 无临时密码 + 密码策略拦截

**现象**: `grep 'temporary password' /var/log/mysqld.log` 无输出；设简单密码报 `ERROR 1819`。

**根因**: MySQL 已有历史数据目录不生成临时密码；密码策略要求 `大小写+数字+特殊字符 ≥ 8 位`。

**解决**:
```bash
systemctl stop mysqld
mysqld --user=mysql --skip-grant-tables & sleep 3

mysql -uroot <<SQL
FLUSH PRIVILEGES;
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Root@123456';
exit
SQL

pkill mysqld && systemctl start mysqld
```

> 💡 `mysql_native_password` 是新老客户端兼容的关键参数。

---

### 🟥 问题 3: JAVA_HOME 路径陷阱 — `$JAVA_HOME/bin/java` 不存在

**现象**: Hadoop/Hive 所有依赖 JAVA_HOME 的组件全部报错：`bin/java is not executable`。

**根因**: CentOS OpenJDK RPM 包中 `java` 实际路径是 `jre/bin/java`，而非脚本默认查找的 `bin/java`。

**排查**:
```bash
which java                  # /usr/bin/java
readlink -f $(which java)   # → .../jre/bin/java  (注意 jre/)
ls $JAVA_HOME/bin/java      # 文件不存在！
```

**解决** — 软链接桥接:
```bash
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.262.b10-1.el7.x86_64
mkdir -p ${JAVA_HOME}/bin
ln -sf ${JAVA_HOME}/jre/bin/java ${JAVA_HOME}/bin/java
```

> 💡 OpenJDK RPM 和 tar.gz 目录结构不同，永远先 `readlink -f` 确认路径。

---

### 🟥 问题 4: Shell 脚本跨平台换行符 (`\r`)

**现象**: Windows 编辑后传到 Linux 执行报 `未预期的符号 '(' 附近有语法错误`。

**根因**: CRLF (`\r\n`) 换行符不被 bash 识别。

| 尝试 | 结果 |
|------|------|
| `sed 's/\r//'` | ❌ 远程 sh 中 `\r` 不解释为回车 |
| `tr -d '\r'` | ❌ 损坏中文 UTF-8 编码 |

**最终方案**: 在 master 上通过 heredoc 直接向 slave 写脚本，彻底绕过文件传输:
```bash
ssh slave1 "cat > /root/bigdata-deploy/deploy_slave.sh" <<'EOF'
#!/bin/bash
# ... 脚本内容 ...
EOF
```

> 💡 跨平台脚本终极方案：「不在 Windows 上编辑后传到 Linux」。

---

### 🟥 问题 5: Slave → Master SSH 免密缺口

**现象**: `Permission denied (publickey)` — slave 回连 master 拉配置时失败。

**根因**: 只配了 Master → Slave 免密，Slave → Master 没有配。

**解决**: 改为 Master 主动推配置:
```bash
scp /opt/module/hadoop-3.3.6/etc/hadoop/* slave1:/opt/module/hadoop-3.3.6/etc/hadoop/
scp /opt/module/hadoop-3.3.6/etc/hadoop/* slave2:/opt/module/hadoop-3.3.6/etc/hadoop/
```

> 💡 集群配置分发黄金法则：「能推不拉」。

---

### 🟥 问题 6: HiveServer2 端口 10000 始终无法监听

**现象**: 进程存在但端口从未监听，`beeline` 连接被拒。Metastore 正常工作。

**决策**: 放弃 HiveServer2。Hive CLI 直连 Metastore 功能完全等价:
```bash
hive -e "show databases;"
hive -f /path/to/etl_script.sql
```

> 💡 学习项目中不与次要组件死磕，HiveServer2 仅用于远程 JDBC。

---

### 🟥 问题 7: Kafka 连接 ZooKeeper 超时 — 防火墙封锁

**现象**: `ZooKeeperClientTimeoutException` — ZK 进程/端口正常但跨节点不通。

**排查**: `nc slave1 2181` 无响应 → `iptables -L -n` 发现规则链。

**解决**:
```bash
for host in slave1 slave2; do
    ssh $host "systemctl stop firewalld; systemctl disable firewalld; iptables -F"
done
```

> 💡 集群组件连接超时，**先查防火墙**。

---

### 🟥 问题 8: Spark HistoryServer 启动报错

**现象**: `FileNotFoundException: File file:/tmp/spark-events does not exist`

**解决**: `mkdir -p /tmp/spark-events` — 30 秒搞定。

---

### 🟥 问题 9: Superset 版本降级

**现象**: 计划装 Superset 4.0，pip 自动降级到 `0.38.1`。

**根因**: CentOS 7 自带 Python 3.6，Superset 4.x 需要 Python 3.8+。

**结果**: 0.38.1 功能满足报表需求，后续可用 Docker 部署新版。

---

### 🟥 问题 10: DolphinScheduler Web UI 登录表单不显示

**现象**: `http://master:12345/dolphinscheduler/ui/` 有标题无登录框，F12 报错:
```
TypeError: a.replaceAll is not a function
```

**根因**: DS 3.2.x 前端用了 ES2021 的 `String.replaceAll()`，旧浏览器不支持。

**解决**:
1. 换用 Chrome ≥ 85 或 Edge ≥ 85
2. 如果 VM 内 Firefox 不行，将 IP 映射到宿主机 hosts：
```
# Windows: C:\Windows\System32\drivers\etc\hosts
192.168.254.129   master
```
3. 用宿主机最新浏览器访问

> 💡 大数据 Web 组件前端迭代快，旧浏览器遇 JS 报错先查兼容性。

---

## 💡 项目亮点

本项目覆盖大数据工程师的核心技能栈，从底层集群搭建到上层调度可视化全链路贯通：

| 技术领域 | 实践内容 |
|----------|----------|
| 🏗️ **集群架构** | 3 节点 CentOS 7 搭建 HDFS 3.3.6 + YARN + Hive 3.1.3 + Spark 3.4.3 全栈集群 |
| 🔄 **版本兼容** | 解决 guava 冲突、OpenJDK RPM 路径适配、Python 3.6 天花板等兼容性难题 |
| 🛡️ **故障诊断** | ZooKeeper 连接超时、Kafka 防火墙封锁、DolphinScheduler 前端 JS 兼容性等 10 个真实排错案例 |
| 🗄️ **MySQL 管理** | 8.0 密码策略绕过、元数据库初始化、多用户权限配置 |
| 📨 **消息队列** | Kafka 3 Broker 集群搭建、Topic 管理、ZooKeeper 协调 |
| 🤖 **运维自动化** | 6 个 Shell 脚本覆盖部署/启停/巡检全流程，按依赖顺序编排 12 个服务 |
| 📦 **EOL 适配** | CentOS 7 归档源切换、MySQL GPG Key 更新，保障已停服系统的可用性 |

---

## 🗺️ Roadmap

- [x] **数仓分层建模** — MySQL 业务表 → DataX 同步 → Hive ODS → DWD → DWS → ADS
- [x] **ETL 流程开发** — Hive SQL 清洗、汇总、宽表生成
- [x] **DolphinScheduler 任务编排** — 每日定时跑 ETL
- [x] **Superset 看板** — ADS 层数据可视化

## 🔗 关联项目

| 项目 | 说明 |
|------|------|
| [bigdata-data-dev](https://github.com/XiaYeWan/bigdata-data-dev) | 数据采集与实时处理（Day2） |
| [bigdata-data-warehouse](https://github.com/XiaYeWan/bigdata-data-warehouse) | 离线数仓ETL + DS调度 + Superset可视化（Day3） |

---

## 📄 License

MIT © 2026 BigData-Deploy Contributors

---

> 📦 **环境**: CentOS 7 + OpenJDK 1.8 + 3 台 VMware 虚拟机  
> 📅 **装机日期**: 2026-05-17  
> ⭐ **项目状态**: 底层设施 ✅ | 数仓模型 ✅ | DS 调度 ✅ | Superset 可视化 ✅
