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
package org.apache.spark.deploy.k8s.features

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{HasMetadata, ServiceBuilder}

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH, KUBERNETES_DRIVER_SERVICE_IP_FAMILIES, KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY, KUBERNETES_DRIVER_SERVICE_PUBLISH_NOT_READY_ADDRESSES, KUBERNETES_DRIVER_UI_SERVICE_ENABLED, KUBERNETES_DRIVER_UI_SERVICE_NAME, KUBERNETES_DRIVER_UI_SERVICE_TYPE}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}

private[spark] class DriverServiceFeatureStep(
    kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {
  import DriverServiceFeatureStep._

  require(kubernetesConf.getOption(DRIVER_BIND_ADDRESS_KEY).isEmpty,
    s"$DRIVER_BIND_ADDRESS_KEY is not supported in Kubernetes mode, as the driver's bind " +
      "address is managed and set to the driver pod's IP address.")
  require(kubernetesConf.getOption(DRIVER_HOST_KEY).isEmpty,
    s"$DRIVER_HOST_KEY is not supported in Kubernetes mode, as the driver's hostname will be " +
      "managed via a Kubernetes service.")

  private val resolvedServiceName = kubernetesConf.driverServiceName
  private val ipFamilyPolicy =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY)
  private val ipFamilies =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILIES).split(",").toList.asJava
  private val publishNotReadyAddresses =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_PUBLISH_NOT_READY_ADDRESSES)

  private val driverPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_PORT.key, DEFAULT_DRIVER_PORT)
  private val driverBlockManagerPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_BLOCK_MANAGER_PORT.key, DEFAULT_BLOCKMANAGER_PORT)
  private val  driverUIPort = kubernetesConf.get(config.UI.UI_PORT)
  private val driverSparkConnectServerPort = kubernetesConf.sparkConf.getInt(
    CONNECT_GRPC_BINDING_PORT, DEFAULT_SPARK_CONNECT_SERVER_PORT)

  // Optional dedicated UI Service (ClusterIP by default), suitable as an Ingress backend.
  // See KUBERNETES_DRIVER_UI_SERVICE_ENABLED.
  private val uiServiceEnabled = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_ENABLED)
  private lazy val uiServiceType = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_TYPE)
  private lazy val uiServiceName: String = {
    // Prefer an explicit name (typically injected by an external controller like Spark Operator
    // so its Ingress backend and Spark's service agree on one name). Otherwise derive it.
    kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_NAME).getOrElse {
      val preferred = s"${kubernetesConf.resourceNamePrefix}$DRIVER_UI_SVC_POSTFIX"
      if (preferred.length <= KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH) preferred
      else {
        val shorter = s"spark-${kubernetesConf.appId}$DRIVER_UI_SVC_POSTFIX"
        logWarning(s"Preferred UI service name '$preferred' exceeds Kubernetes DNS label limit " +
          s"($KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH); using '$shorter' instead.")
        shorter
      }
    }
  }

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val driverHostname = s"$resolvedServiceName.${kubernetesConf.namespace}.svc"
    val base = Map(DRIVER_HOST_KEY -> driverHostname,
      config.DRIVER_PORT.key -> driverPort.toString,
      config.DRIVER_BLOCK_MANAGER_PORT.key -> driverBlockManagerPort.toString)
    // Expose the UI Service name so the driver-side patcher can look it up.
    if (uiServiceEnabled) {
      base + (KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL -> uiServiceName)
    } else {
      base
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val driverService = new ServiceBuilder()
      .withNewMetadata()
        .withName(resolvedServiceName)
        .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
        .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
        .addToLabels(kubernetesConf.serviceLabels.asJava)
        .endMetadata()
      .withNewSpec()
        .withClusterIP("None")
        .withPublishNotReadyAddresses(publishNotReadyAddresses)
        .withIpFamilyPolicy(ipFamilyPolicy)
        .withIpFamilies(ipFamilies)
        .withSelector(kubernetesConf.labels.asJava)
        .addNewPort()
          .withName(DRIVER_PORT_NAME)
          .withPort(driverPort)
          .withNewTargetPort(driverPort)
          .endPort()
        .addNewPort()
          .withName(BLOCK_MANAGER_PORT_NAME)
          .withPort(driverBlockManagerPort)
          .withNewTargetPort(driverBlockManagerPort)
          .endPort()
        .addNewPort()
          .withName(UI_PORT_NAME)
          .withPort(driverUIPort)
          .withNewTargetPort(driverUIPort)
          .endPort()
        .addNewPort()
          .withName(SPARK_CONNECT_SERVER_PORT_NAME)
          .withPort(driverSparkConnectServerPort)
          .withNewTargetPort(driverSparkConnectServerPort)
          .withAppProtocol("grpc")
          .endPort()
        .endSpec()
      .build()

    // Optionally add a dedicated ClusterIP/NodePort/LoadBalancer Service exposing only the UI
    // port. Unlike the headless driver service above, this one is suitable as an Ingress
    // backend. Its targetPort is set to the configured `spark.ui.port` at creation time; after
    // the driver Jetty binds, `K8sDriverUIServicePatcher` updates targetPort to the actual
    // bound port. See KUBERNETES_DRIVER_UI_SERVICE_ENABLED and Flink FLINK-24947.
    val extra: Seq[HasMetadata] = if (uiServiceEnabled) {
      val uiService = new ServiceBuilder()
        .withNewMetadata()
          .withName(uiServiceName)
          .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
          .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
          .addToLabels(kubernetesConf.serviceLabels.asJava)
          .endMetadata()
        .withNewSpec()
          .withType(uiServiceType)
          .withSelector(kubernetesConf.labels.asJava)
          .addNewPort()
            .withName(UI_PORT_NAME)
            .withPort(driverUIPort)
            .withNewTargetPort(driverUIPort)   // placeholder; patched at runtime
            .endPort()
          .endSpec()
        .build()
      Seq(uiService)
    } else Seq.empty

    Seq(driverService) ++ extra
  }
}

private[spark] object DriverServiceFeatureStep {
  val DRIVER_BIND_ADDRESS_KEY = config.DRIVER_BIND_ADDRESS.key
  val DRIVER_HOST_KEY = config.DRIVER_HOST_ADDRESS.key
  val DRIVER_SVC_POSTFIX = "-driver-svc"
  val MAX_SERVICE_NAME_LENGTH = KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH

  /**
   * Internal spark conf key used to pass the UI service name from feature step to the
   * driver runtime (SparkContext) so the patcher can look up the Service to patch.
   * Not user-facing.
   */
  val KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL =
    "spark.kubernetes.driver.ui.service.name.internal"
}
