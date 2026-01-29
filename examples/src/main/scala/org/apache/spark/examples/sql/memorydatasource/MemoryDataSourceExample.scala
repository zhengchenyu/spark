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

// scalastyle:off println
package org.apache.spark.examples.sql.memorydatasource

import org.apache.spark.sql.SparkSession

/**
 * MemoryDataSource usage example
 *
 * Demonstrates how to use custom MemoryDataSource, including:
 * - Catalog management
 * - DataSource V2 interface
 * - Extension mechanism
 * - Predicate pushdown optimization
 */
object MemoryDataSourceExample {

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession with custom Catalog and Extension
    val spark = SparkSession.builder()
      .appName("Memory DataSource Example")
      .master("local[2]")
      // Configure custom Catalog
      .config("spark.sql.catalog.memory",
        "org.apache.spark.examples.sql.memorydatasource.MemoryCatalog")
      // Configure extension
      .config("spark.sql.extensions",
        "org.apache.spark.examples.sql.memorydatasource.MemoryExtension")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println("\n" + "="*60)
      println("Step 1: Create table")
      println("="*60)
      spark.sql("""
        CREATE TABLE memory.default.users (
          id INT,
          name STRING,
          age INT,
          city STRING
        ) USING org.apache.spark.examples.sql.memorydatasource.MemoryTableProvider
      """)

      println("\n" + "="*60)
      println("Step 2: Insert data")
      println("="*60)
      spark.sql("""
        INSERT INTO memory.default.users VALUES
          (1, 'Alice', 30, 'Beijing'),
          (2, 'Bob', 25, 'Shanghai'),
          (3, 'Charlie', 35, 'Beijing'),
          (4, 'David', 28, 'Shanghai'),
          (5, 'Eve', 32, 'Guangzhou')
      """)

      println("\n" + "="*60)
      println("Step 3: Query all data")
      println("="*60)
      spark.sql("SELECT * FROM memory.default.users").show()

      println("\n" + "="*60)
      println("Step 4: Query with filter (test predicate pushdown)")
      println("="*60)
      spark.sql("SELECT * FROM memory.default.users WHERE city = 'Beijing'").show()

      println("\n" + "="*60)
      println("Step 5: Aggregation query")
      println("="*60)
      spark.sql(
        "SELECT city, COUNT(*), AVG(age) FROM memory.default.users GROUP BY city").show()

      println("\n" + "="*60)
      println("Step 6: View execution plan")
      println("="*60)
      spark.sql("SELECT * FROM memory.default.users WHERE age > 28").explain(true)

      println("\n" + "="*60)
      println("Step 7: Test Extension optimization (constant folding)")
      println("="*60)
      println("Query: WHERE age > (20 + 10)")
      println("Expected: Optimizer will fold (20 + 10) => 30 at compile time")
      spark.sql("SELECT * FROM memory.default.users WHERE age > (20 + 10)").show()

      println("\n" + "="*60)
      println("Step 8: List all tables")
      println("="*60)
      spark.sql("SHOW TABLES IN memory.default").show()

    } finally {
      spark.stop()
    }
  }
}
// scalastyle:on println
