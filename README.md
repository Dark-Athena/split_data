# 分片 SQL 生成工具

输入表名与分片数，程序拉取主键数据，在内存计算切片边界并输出互不重叠的 WHERE 语句。支持 PostgreSQL/GaussDB/openGauss 与 Oracle。

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Dark-Athena/split_data)

## 环境与安装
1. 准备 Python 3.9+。
2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
3. 数据库驱动：
   - PostgreSQL: 使用 `psycopg2-binary`（已在依赖中）。
   - Oracle: 使用 `cx_Oracle`（已在依赖中）；需要本地 Oracle Instant Client 可用。

## 使用示例
- PostgreSQL：
  ```bash
  python python/slice_sql.py \
    --dbtype pg \
    --conn "dbname=demo user=demo password=demo host=127.0.0.1 port=5432" \
    --table public.my_table \
    --slices 8
  ```
  或 Java 版（需先将 JDBC 驱动放入 classpath）：
  ```bash
  javac -cp "java/lib/*" java/SliceSql.java
  java -cp ".:java/lib/*:java" SliceSql \
    --dbtype=pg \
    --url="jdbc:postgresql://127.0.0.1:5432/demo" \
    --user=demo --password=demo \
    --table=public.my_table --slices=8
  ```
- Oracle（EZ Connect）：
  ```bash
  python python/slice_sql.py \
    --dbtype ora \
    --conn "demo/demo@127.0.0.1:1521/XEPDB1" \
    --table MY_TABLE \
    --slices 8
  ```
  或 Java 版：
  ```bash
  javac -cp "java/lib/*" java/SliceSql.java
  java -cp ".:java/lib/*:java" SliceSql \
    --dbtype=ora \
    --url="jdbc:oracle:thin:@127.0.0.1:1521/XEPDB1" \
    --user=demo --password=demo \
    --table=MY_TABLE --slices=8
  ```

程序会打印多条 `SELECT * FROM <table> WHERE ...;`，每条即一个分片 SQL。

> Windows 运行 Java 时请将 classpath 分隔符 `:` 改为 `;`。

## 运行时说明
- 单列主键：使用分位点生成近似等量的范围 `[lo, hi)`，最后一片右闭确保无遗漏。
- 复合主键：在内存按词典序排序后按行数均分，区间为左闭右开（末片右闭），覆盖全表且不重叠。

## 参数
- `--dbtype`：`pg` 或 `ora`。
- `--conn`：传递给驱动 `connect()` 的连接串。
- `--table`：表名，可带 schema（如 `public.my_table` 或 `SCHEMA.TABLE`）。
- `--log-level`：日志级别（DEBUG/INFO/WARNING/ERROR），默认 INFO。
- `--log-file`：日志文件路径，默认 `slice_sql.log`。
- `--slices`：期望分片数量（默认 8）。

## 注意
- 输出 SQL 已内嵌字面量边界，便于直接执行。
- 获取主键时使用默认一致性读；如需严格一致性，请在调用端控制事务隔离级别并在同一快照内执行分片查询。

## 严正声明
> **【重要法律提示】 本项目实验性代码的实现思路，可能与尚在审查中的中国专利申请 CN116521708A 存在相似的部分。该专利申请目前未获授权，后续可能被修改或驳回。本代码仅用于个人技术研究交流，请勿用于任何商业目的。使用者需自行关注该专利的法律状态并评估风险。**