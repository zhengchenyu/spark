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
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Custom Catalog implementation
 *
 * This file contains:
 * - MemoryCatalog: Main catalog for managing table metadata
 * - MemoryStore: In-memory storage for table data and schemas
 */
class MemoryCatalog extends TableCatalog with SupportsNamespaces {

  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    // scalastyle:off println
    println(s"[MemoryCatalog] Initialize Catalog: $name")
    // scalastyle:on println
  }

  override def name(): String = catalogName

  // ========== Namespace Operations ==========

  override def listNamespaces(): Array[Array[String]] = {
    Array(Array("default"))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) {
      listNamespaces()
    } else {
      Array.empty
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (namespace.length == 1 && namespace(0) == "default") {
      Map("comment" -> "Default namespace").asJava
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(namespace: Array[String],
                               metadata: util.Map[String, String]): Unit = {
    if (namespace.length != 1 || namespace(0) != "default") {
      throw new NamespaceAlreadyExistsException(namespace)
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    if (namespace.length != 1 || namespace(0) != "default") {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    false // Do not allow deleting default namespace
  }

  // ========== Table Operations ==========

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    // scalastyle:off println
    println(s"[MemoryCatalog] List tables: namespace=${namespace.mkString(".")}")
    // scalastyle:on println
    MemoryStore.listTables().map(Identifier.of(namespace, _)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.name()
    // scalastyle:off println
    println(s"[MemoryCatalog] Load table: $tableName")
    // scalastyle:on println

    MemoryStore.getSchema(tableName) match {
      case Some(schema) =>
        new MemoryTable(tableName, schema)
      case None =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val tableName = ident.name()
    // scalastyle:off println
    println(s"[MemoryCatalog] Create table: $tableName")
    // scalastyle:on println

    if (MemoryStore.tableExists(tableName)) {
      throw new TableAlreadyExistsException(ident)
    }

    val schema = columnsToStructType(columns)
    MemoryStore.createTable(tableName, schema)

    new MemoryTable(tableName, schema)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // scalastyle:off println
    println(s"[MemoryCatalog] Alter table: ${ident.name()}")
    // scalastyle:on println
    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableName = ident.name()
    // scalastyle:off println
    println(s"[MemoryCatalog] Drop table: $tableName")
    // scalastyle:on println
    MemoryStore.dropTable(tableName)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Rename not supported")
  }

  /**
   * Convert DataSource V2 columns to StructType
   * This is a simplified version of CatalogV2Util.v2ColumnsToStructType
   */
  private def columnsToStructType(columns: Array[Column]): StructType = {
    import org.apache.spark.sql.types.{Metadata, StructField}

    val fields = columns.map { col =>
      val metadata = Option(col.metadataInJSON())
        .map(Metadata.fromJson)
        .getOrElse(Metadata.empty)

      var field = StructField(col.name(), col.dataType(), col.nullable(), metadata)

      // Add comment if present
      Option(col.comment()).foreach { comment =>
        field = field.withComment(comment)
      }

      field
    }

    StructType(fields)
  }
}

/**
 * In-memory storage using ConcurrentHashMap to store table data
 *
 * This object provides thread-safe storage for:
 * - Table schemas (tableName -> StructType)
 * - Table data (tableName -> List[InternalRow])
 */
object MemoryStore {

  // Store all table data: tableName -> List[InternalRow]
  private val tables = new ConcurrentHashMap[String, java.util.List[InternalRow]]()

  // Store all table schemas: tableName -> StructType
  private val schemas = new ConcurrentHashMap[String, StructType]()

  def createTable(tableName: String, schema: StructType): Unit = {
    schemas.put(tableName, schema)
    tables.put(tableName, new java.util.concurrent.CopyOnWriteArrayList[InternalRow]())
  }

  def dropTable(tableName: String): Boolean = {
    schemas.remove(tableName) != null && tables.remove(tableName) != null
  }

  def tableExists(tableName: String): Boolean = {
    schemas.containsKey(tableName)
  }

  def getSchema(tableName: String): Option[StructType] = {
    Option(schemas.get(tableName))
  }

  def getData(tableName: String): Seq[InternalRow] = {
    Option(tables.get(tableName)).map(_.asScala.toSeq).getOrElse(Seq.empty)
  }

  def insertData(tableName: String, rows: Seq[InternalRow]): Unit = {
    val table = tables.get(tableName)
    if (table != null) {
      rows.foreach(table.add)
    }
  }

  def clearData(tableName: String): Unit = {
    val table = tables.get(tableName)
    if (table != null) {
      table.clear()
    }
  }

  def listTables(): Seq[String] = {
    schemas.keys().asScala.toSeq
  }
}
