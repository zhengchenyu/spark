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
package org.apache.spark.scheduler.cluster.k8s

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{IntOrString, ServiceBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.deploy.k8s.Constants.UI_PORT_NAME
import org.apache.spark.internal.Logging

/**
 * Driver-side utility that updates the `targetPort` of the dedicated Spark UI Kubernetes Service
 * (created by [[org.apache.spark.deploy.k8s.features.DriverUIServiceFeatureStep]]) to the actual
 * port the driver's Jetty server bound to.
 *
 * <p>Called from [[KubernetesClusterSchedulerBackend.start]] after `SparkContext` has initialized
 * and the Web UI has bound. Only takes effect when the dedicated UI service was created
 * (i.e. `spark.kubernetes.driver.ui.service.enabled=true`).
 *
 * <p>Mirrors the design of Flink's
 * `KubernetesResourceManagerDriver.updateKubernetesServiceTargetPortIfNecessary` (FLINK-24947).
 *
 * <p>Requires the driver's ServiceAccount to have `patch services` permission.
 */
private[k8s] object K8sDriverUIServicePatcher extends Logging {

  /**
   * Patch the targetPort of the given Service's `spark-ui` port entry to `actualPort`, if the
   * current value differs.
   *
   * @param client       Kubernetes client to use (typically the one already held by the backend).
   * @param namespace    Namespace where the Service lives.
   * @param serviceName  Name of the UI Service (from
   *                     `spark.kubernetes.driver.ui.service.name.internal`).
   * @param actualPort   The actual port the driver's Jetty server bound to
   *                     (typically `SparkUI.boundPort`).
   */
  def patchTargetPort(
      client: KubernetesClient,
      namespace: String,
      serviceName: String,
      actualPort: Int): Unit = {
    logInfo(s"K8sDriverUIServicePatcher.patchTargetPort invoked; " +
      s"namespace='$namespace', serviceName='$serviceName', actualPort=$actualPort")
    try {
      val service = client.services()
        .inNamespace(namespace)
        .withName(serviceName)
        .get()

      if (service == null) {
        logWarning(s"UI service '$serviceName' not found in namespace '$namespace'; " +
          "skipping targetPort patch. This should only happen if the service was deleted " +
          "externally after driver pod creation.")
        return
      }

      logInfo(s"Fetched UI service '$serviceName': " +
        s"type=${service.getSpec.getType}, " +
        s"clusterIP=${service.getSpec.getClusterIP}, " +
        s"ports=${service.getSpec.getPorts.asScala.map(p =>
          s"${p.getName}(port=${p.getPort},targetPort=${p.getTargetPort.getStrVal match {
            case null => p.getTargetPort.getIntVal.toString
            case s => s
          }})").mkString(",")}")

      val currentTargetPort = service.getSpec.getPorts.asScala
        .find(_.getName == UI_PORT_NAME)
        .map(_.getTargetPort.getIntVal.toInt)

      currentTargetPort match {
        case Some(existing) if existing == actualPort =>
          logInfo(s"UI service '$serviceName' targetPort already matches actual UI port " +
            s"($actualPort); no patch needed.")

        case _ =>
          logInfo(s"Preparing to patch UI service '$serviceName' targetPort: " +
            s"current=${currentTargetPort.map(_.toString).getOrElse("<unset>")}, " +
            s"actualPort=$actualPort")
          val updated = new ServiceBuilder(service)
            .editSpec()
              .editMatchingPort(portBuilder => portBuilder.build().getName == UI_PORT_NAME)
                .withTargetPort(new IntOrString(actualPort))
                .endPort()
              .endSpec()
            .build()

          client.services()
            .inNamespace(namespace)
            .withName(serviceName)
            .patch(updated)

          logInfo(s"Patched UI service '$serviceName' targetPort " +
            s"${currentTargetPort.map(_.toString).getOrElse("<unset>")} -> $actualPort")
      }
    } catch {
      case e: Exception =>
        // Fail-fast: bubble up so the caller can decide (typically driver will exit and let the
        // controller retry). Do NOT silently swallow — that would leave UI Ingress pointing at
        // the wrong port and be very hard to diagnose.
        logError(s"Failed to patch UI service '$serviceName' targetPort to $actualPort " +
          s"(namespace='$namespace'). Check that the driver's ServiceAccount has " +
          s"'patch services' permission and that the Service exists.", e)
        throw e
    }
  }
}
