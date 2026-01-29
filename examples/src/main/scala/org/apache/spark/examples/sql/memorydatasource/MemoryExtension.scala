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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * Custom extension for Spark SQL with practical optimization rules
 *
 * This extension demonstrates:
 * 1. Constant folding (compile-time evaluation)
 * 2. Redundant filter elimination
 * 3. Query rewrite optimization
 */
class MemoryExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // scalastyle:off println
    println("[MemoryExtension] Register extension")
    // scalastyle:on println

    // Inject optimizer rules
    extensions.injectOptimizerRule { session =>
      new MemoryOptimizerRule(session)
    }
  }
}

/**
 * Custom optimizer rule with practical optimizations
 *
 * Optimizations include:
 * - Constant folding: WHERE age > (10 + 20) => WHERE age > 30
 * - Redundant filter elimination: WHERE a > 10 AND a > 10 => WHERE a > 10
 * - Trivial filter removal: WHERE true => (no filter)
 */
case class MemoryOptimizerRule(spark: org.apache.spark.sql.SparkSession)
    extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Apply constant folding first
    val withConstantFolding = plan.transformAllExpressions {
      case Add(Literal(v1: Int, t1), Literal(v2: Int, _), _) =>
        val result = v1 + v2
        // scalastyle:off println
        println(s"[MemoryOptimizer] Constant folding: $v1 + $v2 => $result")
        // scalastyle:on println
        Literal(result, t1)

      case Subtract(Literal(v1: Int, t1), Literal(v2: Int, _), _) =>
        val result = v1 - v2
        // scalastyle:off println
        println(s"[MemoryOptimizer] Constant folding: $v1 - $v2 => $result")
        // scalastyle:on println
        Literal(result, t1)
    }

    // Apply filter optimizations
    withConstantFolding.transformDown {
      // Remove trivial filters: WHERE true
      case Filter(Literal(true, _), child) =>
        // scalastyle:off println
        println(s"[MemoryOptimizer] Remove trivial filter: WHERE true")
        // scalastyle:on println
        child

      // Simplify redundant filters
      case Filter(condition, child) =>
        val simplified = simplifyCondition(condition)
        if (simplified != condition) {
          Filter(simplified, child)
        } else {
          Filter(condition, child)
        }
    }
  }

  private def simplifyCondition(expr: Expression): Expression = {
    expr match {
      // Remove duplicate AND conditions: a > 10 AND a > 10 => a > 10
      case And(left, right) if left.semanticEquals(right) =>
        // scalastyle:off println
        println(s"[MemoryOptimizer] Remove duplicate condition")
        // scalastyle:on println
        left

      // Recursively simplify nested expressions
      case And(left, right) =>
        And(simplifyCondition(left), simplifyCondition(right))

      case Or(left, right) =>
        Or(simplifyCondition(left), simplifyCondition(right))

      case _ => expr
    }
  }
}
