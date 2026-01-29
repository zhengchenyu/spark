# Memory DataSource Example

这是一个完整的自定义 DataSource V2 实现示例，用于演示 Spark 的核心机制：
- **Catalog** - 表元数据管理
- **DataSource V2** - 数据读写接口
- **Extension** - 扩展机制

## 文件说明

### 核心组件

1. **MemoryStore.scala** - 数据存储层
   - 使用 `ConcurrentHashMap` 存储表数据和 Schema
   - 提供表的 CRUD 操作

2. **MemoryCatalog.scala** - Catalog 实现
   - 实现 `TableCatalog` 和 `SupportsNamespaces` 接口
   - 管理表的元数据

3. **MemoryTable.scala** - Table 实现
   - 实现 `Table`, `SupportsRead`, `SupportsWrite` 接口
   - 提供读写能力

4. **MemoryTableProvider.scala** - TableProvider 实现
   - 用于 `USING` 子句
   - 创建 Table 实例

### 读取路径

5. **MemoryScanBuilder.scala** - 读取实现
   - `MemoryScanBuilder` - 实现 `ScanBuilder` 和 `SupportsPushDownFilters`
   - `MemoryScan` - 实现 `Scan` 和 `Batch`
   - `MemoryPartitionReader` - 实际的数据读取
   - 支持谓词下推（EqualTo, GreaterThan, LessThan）

### 写入路径

6. **MemoryWriteBuilder.scala** - 写入实现
   - `MemoryWriteBuilder` - 实现 `WriteBuilder`
   - `MemoryBatchWrite` - 实现 `BatchWrite`
   - `MemoryDataWriter` - 实际的数据写入

### 扩展机制

7. **MemoryExtension.scala** - Extension 实现
   - 实现 `SparkSessionExtensions`
   - 注入自定义优化规则

### 示例程序

8. **MemoryDataSourceExample.scala** - 主程序
   - 演示如何使用 MemoryDataSource
   - 包含完整的 CRUD 操作示例

## 如何运行

### 方法 1: 在 IDEA 中运行

1. 在 IDEA 中打开 Spark 项目
2. 找到 `MemoryDataSourceExample.scala`
3. 右键 -> Run 'MemoryDataSourceExample'

### 方法 2: 使用 Maven 编译

```bash
# 编译 examples 模块
mvn clean compile -pl examples -am -DskipTests

# 运行示例
mvn exec:java -pl examples \
  -Dexec.mainClass="org.apache.spark.examples.sql.memorydatasource.MemoryDataSourceExample"
```

### 方法 3: 使用 spark-submit

```bash
# 先打包
mvn clean package -pl examples -am -DskipTests

# 运行
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.examples.sql.memorydatasource.MemoryDataSourceExample \
  --master local[2] \
  examples/target/spark-examples_2.12-3.5.7.jar
```

## 调试指南

### 关键断点位置

1. **Catalog 初始化**
   - `MemoryCatalog.initialize()` - 查看 Catalog 如何被加载

2. **表创建**
   - `MemoryCatalog.createTable()` - 查看表创建流程

3. **表加载**
   - `MemoryCatalog.loadTable()` - 查看表加载流程

4. **读取路径**
   - `MemoryTable.newScanBuilder()` - 读取入口
   - `MemoryScanBuilder.pushFilters()` - 谓词下推
   - `MemoryPartitionReader.next()/get()` - 数据读取

5. **写入路径**
   - `MemoryTable.newWriteBuilder()` - 写入入口
   - `MemoryDataWriter.write()` - 数据写入
   - `MemoryBatchWrite.commit()` - 提交写入

6. **扩展机制**
   - `MemoryExtension.apply()` - 扩展注册
   - `MemoryOptimizerRule.apply()` - 优化规则执行

### 查看执行计划

在示例程序中，可以使用 `explain(true)` 查看完整的执行计划：

```scala
spark.sql("SELECT * FROM memory.default.users WHERE age > 28").explain(true)
```

这会显示：
- Parsed Logical Plan
- Analyzed Logical Plan
- Optimized Logical Plan
- Physical Plan

## 示例输出

运行 `MemoryDataSourceExample` 会看到类似输出：

```
============================================================
步骤 1: 创建表
============================================================
[MemoryCatalog] 初始化 Catalog: memory
[MemoryCatalog] 创建表: users

============================================================
步骤 2: 插入数据
============================================================
[MemoryTable] 创建 WriteBuilder: users
[MemoryDataWriter] 提交分区 0, 任务 0, 行数=5
[MemoryBatchWrite] 提交写入: users, 消息数=1
[MemoryBatchWrite] 插入 5 行数据

============================================================
步骤 3: 查询所有数据
============================================================
[MemoryCatalog] 加载表: users
[MemoryTable] 创建 ScanBuilder: users
[MemoryScan] 规划输入分区: users
+---+-------+---+---------+
| id|   name|age|     city|
+---+-------+---+---------+
|  1|  Alice| 30|  Beijing|
|  2|    Bob| 25| Shanghai|
|  3|Charlie| 35|  Beijing|
|  4|  David| 28| Shanghai|
|  5|    Eve| 32|Guangzhou|
+---+-------+---+---------+

============================================================
步骤 4: 带过滤条件的查询（测试谓词下推）
============================================================
[MemoryScanBuilder] 下推 Filters: EqualTo(city,Beijing)
+---+-------+---+-------+
| id|   name|age|   city|
+---+-------+---+-------+
|  1|  Alice| 30|Beijing|
|  3|Charlie| 35|Beijing|
+---+-------+---+-------+
```

## 学习要点

通过这个示例，您可以学习到：

1. **Catalog 机制**
   - 如何实现 `TableCatalog` 接口
   - 如何管理表的元数据
   - Namespace 的概念

2. **DataSource V2 API**
   - `TableProvider` 的作用
   - `Table` 接口的实现
   - `ScanBuilder` 和 `WriteBuilder` 的工作流程

3. **谓词下推**
   - 如何实现 `SupportsPushDownFilters`
   - Spark Filter 如何传递给数据源
   - 如何在数据源层面应用过滤条件

4. **数据读写流程**
   - 从 SQL 到实际数据读取的完整路径
   - 分区、任务、提交的概念
   - `InternalRow` 的使用

5. **Extension 机制**
   - 如何实现 `SparkSessionExtensions`
   - 如何注入自定义规则
   - 优化规则的执行时机

## 扩展方向

基于这个示例，您可以：

1. **添加更多谓词下推支持**
   - 支持 `In`, `IsNull`, `IsNotNull` 等
   - 支持复杂的 `And`, `Or` 组合

2. **实现列裁剪**
   - 实现 `SupportsPushDownRequiredColumns`
   - 只读取需要的列

3. **实现聚合下推**
   - 实现 `SupportsPushDownAggregates`
   - 在数据源层面进行聚合

4. **添加分区支持**
   - 实现多分区读取
   - 支持分区裁剪

5. **持久化存储**
   - 将数据存储到文件系统
   - 将元数据存储到数据库

6. **实现 UPDATE/DELETE**
   - 添加 `SupportsDelete` 和 `SupportsUpdate`
   - 实现行级别的修改

## 相关文档

详细的原理分析请参考：`aidocs/spark_datasource_v2_guide.md`

