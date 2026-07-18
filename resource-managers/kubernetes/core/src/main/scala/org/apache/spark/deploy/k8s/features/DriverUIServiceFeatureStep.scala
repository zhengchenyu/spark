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
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH, KUBERNETES_DRIVER_UI_SERVICE_ENABLED, KUBERNETES_DRIVER_UI_SERVICE_NAME, KUBERNETES_DRIVER_UI_SERVICE_TYPE}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}

/**
 * Optionally provisions a dedicated Kubernetes Service exposing only the Spark driver's Web UI
 * port. Unlike the headless service produced by [[DriverServiceFeatureStep]], this one is a
 * regular Service (ClusterIP by default) suitable as an Ingress backend.
 *
 * At creation time, the Service's `targetPort` is set to the configured `spark.ui.port`
 * (a placeholder when the user has requested a random port). Once the driver's Jetty server
 * has bound, `K8sDriverUIServicePatcher` updates the Service's `targetPort` to reflect the
 * actual bound port. See [[KUBERNETES_DRIVER_UI_SERVICE_ENABLED]] and Flink FLINK-24947.
 *
 * This step is orthogonal to [[DriverServiceFeatureStep]]: users may exclude the latter
 * (e.g. combined with `spark.kubernetes.executor.useDriverPodIP=true` for hostNetwork
 * deployments) and keep this step active to retain a stable UI ingress target.
 */
private[spark] class DriverUIServiceFeatureStep(
    kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {
  import DriverUIServiceFeatureStep._

  private val enabled = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_ENABLED)
  private lazy val serviceType = kubernetesConf.get(KUBERNETES_DRIVER_UI_SERVICE_TYPE)
  private lazy val driverUIPort = kubernetesConf.get(config.UI.UI_PORT)

  private lazy val serviceName: String = {
    // Prefer an explicit name (typically injected by an external controller like Spark Operator
    // so its Ingress backend and Spark's Service agree on one name). Otherwise derive it.
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
    if (enabled) {
      Map(KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL -> serviceName)
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!enabled) return Seq.empty

    val uiService = new ServiceBuilder()
      .withNewMetadata()
        .withName(serviceName)
        .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
        .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
        .addToLabels(kubernetesConf.serviceLabels.asJava)
        .endMetadata()
      .withNewSpec()
        .withType(serviceType)
        .withSelector(kubernetesConf.labels.asJava)
        .addNewPort()
          .withName(UI_PORT_NAME)
          .withPort(driverUIPort)
          .withNewTargetPort(driverUIPort)   // placeholder; patched at runtime
          .endPort()
        .endSpec()
      .build()
    Seq(uiService)
  }
}

private[spark] object DriverUIServiceFeatureStep {
  /**
   * Internal spark conf key used to pass the UI service name from this feature step to the
   * driver runtime (SparkContext) so `K8sDriverUIServicePatcher` can look up the Service to
   * patch. Not user-facing.
   */
  val KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL =
    "spark.kubernetes.driver.ui.service.name.internal"
}
