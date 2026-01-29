/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.memorydatasource

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, LessThan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * TableProvider implementation (for USING clause)
 *
 * This file contains all table-related classes:
 * - MemoryTableProvider: Entry point for creating tables
 * - MemoryTable: Table implementation with read/write capabilities
 * - Read path: ScanBuilder, Scan, PartitionReader
 * - Write path: WriteBuilder, Write, BatchWrite, DataWriter
 */
class MemoryTableProvider extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new UnsupportedOperationException(
      "Schema inference not supported. Please provide a schema.")
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val tableName = properties.getOrDefault("tableName", "unknown")
    // scalastyle:off println
    println(s"[MemoryTableProvider] Get table: $tableName")
    // scalastyle:on println

    new MemoryTable(tableName, schema)
  }
}

/**
 * Custom Table implementation with read and write support
 */
class MemoryTable(tableName: String, tableSchema: StructType)
    extends Table with SupportsRead with SupportsWrite {

  override def name(): String = tableName

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.OVERWRITE_BY_FILTER
    ).asJava
  }

  // ========== Read Interface ==========

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // scalastyle:off println
    println(s"[MemoryTable] Create ScanBuilder: $tableName")
    // scalastyle:on println
    new MemoryScanBuilder(tableName, tableSchema)
  }

  // ========== Write Interface ==========

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    // scalastyle:off println
    println(s"[MemoryTable] Create WriteBuilder: $tableName")
    // scalastyle:on println
    new MemoryWriteBuilder(tableName, tableSchema)
  }
}

// ========== Read Path Implementation ==========

/**
 * ScanBuilder implementation with predicate pushdown support
 */
class MemoryScanBuilder(tableName: String, tableSchema: StructType)
    extends ScanBuilder with org.apache.spark.sql.connector.read.SupportsPushDownFilters {

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // scalastyle:off println
    println(s"[MemoryScanBuilder] Push down Filters: ${filters.mkString(", ")}")
    // scalastyle:on println

    // Simple implementation: support EqualTo, GreaterThan, LessThan
    val (supported, unsupported) = filters.partition {
      case _: EqualTo | _: GreaterThan | _: LessThan => true
      case _ => false
    }

    _pushedFilters = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    new MemoryScan(tableName, tableSchema, pushedFilters)
  }
}

/**
 * Scan implementation
 */
class MemoryScan(tableName: String, tableSchema: StructType, filters: Array[Filter])
    extends Scan with Batch {

  override def readSchema(): StructType = tableSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // scalastyle:off println
    println(s"[MemoryScan] Plan input partitions: $tableName")
    // scalastyle:on println
    // Simple implementation: single partition
    Array(new MemoryInputPartition(tableName, filters))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MemoryPartitionReaderFactory(tableSchema)
  }
}

/**
 * InputPartition implementation
 */
class MemoryInputPartition(val tableName: String, val filters: Array[Filter])
    extends InputPartition

/**
 * PartitionReaderFactory implementation
 */
class MemoryPartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val memPartition = partition.asInstanceOf[MemoryInputPartition]
    new MemoryPartitionReader(memPartition.tableName, memPartition.filters, schema)
  }
}

/**
 * PartitionReader implementation
 */
class MemoryPartitionReader(tableName: String, filters: Array[Filter], schema: StructType)
    extends PartitionReader[InternalRow] {

  private val data = MemoryStore.getData(tableName)
  private val filteredData = applyFilters(data, filters)
  private val iterator = filteredData.iterator

  private def applyFilters(rows: Seq[InternalRow], filters: Array[Filter]): Seq[InternalRow] = {
    if (filters.isEmpty) {
      rows
    } else {
      rows.filter { row =>
        filters.forall(matchesFilter(row, _))
      }
    }
  }

  private def matchesFilter(row: InternalRow, filter: Filter): Boolean = {
    filter match {
      case EqualTo(attr, value) =>
        val index = schema.fieldIndex(attr)
        val rowValue = row.get(index, schema(index).dataType)
        (rowValue, value) match {
          case (v1: org.apache.spark.unsafe.types.UTF8String, v2: String) => v1.toString.equals(v2)
          case _ => rowValue == value
        }
      case GreaterThan(attr, value: Int) =>
        val index = schema.fieldIndex(attr)
        row.getInt(index) > value
      case LessThan(attr, value: Int) =>
        val index = schema.fieldIndex(attr)
        row.getInt(index) < value
      case _ => true
    }
  }

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {}
}

// ========== Write Path Implementation ==========

/**
 * WriteBuilder implementation
 */
class MemoryWriteBuilder(tableName: String, tableSchema: StructType) extends WriteBuilder {

  override def build(): Write = {
    new MemoryWrite(tableName, tableSchema)
  }
}

/**
 * Write implementation
 */
class MemoryWrite(tableName: String, tableSchema: StructType) extends Write {

  override def toBatch: BatchWrite = {
    new MemoryBatchWrite(tableName, tableSchema)
  }
}

/**
 * BatchWrite implementation
 */
class MemoryBatchWrite(tableName: String, tableSchema: StructType) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new MemoryDataWriterFactory(tableName)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // scalastyle:off println
    println(s"[MemoryBatchWrite] Commit write: $tableName, messages=${messages.length}")
    // scalastyle:on println

    // Collect all written data
    val allRows = messages.flatMap {
      case msg: MemoryWriterCommitMessage => msg.rows
    }

    // scalastyle:off println
    println(s"[MemoryBatchWrite] Insert ${allRows.length} rows")
    // scalastyle:on println
    MemoryStore.insertData(tableName, allRows.toSeq)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // scalastyle:off println
    println(s"[MemoryBatchWrite] Abort write: $tableName")
    // scalastyle:on println
  }
}

/**
 * DataWriterFactory implementation
 */
class MemoryDataWriterFactory(tableName: String) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new MemoryDataWriter(tableName, partitionId, taskId)
  }
}

/**
 * DataWriter implementation
 */
class MemoryDataWriter(tableName: String, partitionId: Int, taskId: Long)
    extends DataWriter[InternalRow] {

  private val buffer = new ArrayBuffer[InternalRow]()

  override def write(record: InternalRow): Unit = {
    // Copy the data (because Spark reuses InternalRow objects)
    buffer += record.copy()
  }

  override def commit(): WriterCommitMessage = {
    // scalastyle:off println
    println(s"[MemoryDataWriter] Commit partition $partitionId, task $taskId, rows=${buffer.size}")
    // scalastyle:on println
    new MemoryWriterCommitMessage(buffer.toArray)
  }

  override def abort(): Unit = {
    // scalastyle:off println
    println(s"[MemoryDataWriter] Abort partition $partitionId, task $taskId")
    // scalastyle:on println
    buffer.clear()
  }

  override def close(): Unit = {}
}

/**
 * WriterCommitMessage implementation
 */
case class MemoryWriterCommitMessage(rows: Array[InternalRow]) extends WriterCommitMessage
