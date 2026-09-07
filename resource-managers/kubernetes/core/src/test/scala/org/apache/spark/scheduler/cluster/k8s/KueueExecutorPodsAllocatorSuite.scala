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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{GenericKubernetesResource, GenericKubernetesResourceList, ObjectMetaBuilder, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, MixedOperation, NonNamespaceOperation, PodResource, Resource}
import io.fabric8.kubernetes.client.dsl.base.{PatchContext, ResourceDefinitionContext}
import io.fabric8.kubernetes.client.utils.KubernetesSerialization
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, anyString, eq => meq}
import org.mockito.Mockito.{never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesExecutorSpec}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.ManualClock

class KueueExecutorPodsAllocatorSuite extends SparkFunSuite with BeforeAndAfter {

  private val driverPodName = "driver"
  private val appUid = "app-uid-123"
  private val jobUidLabel = "kueue.x-k8s.io/job-uid"
  private val executorPodSet = "executor"
  private val driverPodSet = "driver"

  private type GenericRes = GenericKubernetesResource
  private type GenericList = GenericKubernetesResourceList
  private type GenericMixed =
    MixedOperation[GenericRes, GenericList, Resource[GenericRes]]
  private type GenericNamespaced =
    NonNamespaceOperation[GenericRes, GenericList, Resource[GenericRes]]
  private type GenericFiltered =
    FilterWatchListDeletable[GenericRes, GenericList, Resource[GenericRes]]

  @Mock private var kubernetesClient: KubernetesClient = _
  @Mock private var genericMixed: GenericMixed = _
  @Mock private var genericNamespaced: GenericNamespaced = _
  @Mock private var genericFiltered: GenericFiltered = _
  @Mock private var genericResource: Resource[GenericRes] = _
  @Mock private var genericList: GenericList = _
  @Mock private var watch: Watch = _

  // Pod operation mocks so the base ExecutorPodsAllocator.onNewSnapshots can run.
  @Mock private var pods: MixedOperation[Pod, PodList, PodResource] = _
  @Mock private var podsWithNamespace: NonNamespaceOperation[Pod, PodList, PodResource] = _
  @Mock private var driverPodResource: PodResource = _
  @Mock private var execPodResource: PodResource = _
  @Mock private var executorBuilder: KubernetesExecutorBuilder = _
  @Mock private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var conf: SparkConf = _
  private var secMgr: SecurityManager = _
  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _
  private val serialization = new KubernetesSerialization()

  private def driverPodWithController(controller: Boolean): Pod = {
    val builder = new PodBuilder()
      .withNewMetadata()
        .withName(driverPodName)
      .addNewOwnerReference()
        .withController(controller)
        .withKind("SparkApplication")
        .withName("my-app")
        .withUid(appUid)
      .endOwnerReference()
      .endMetadata()
    builder.build()
  }

  private def podSetMap(name: String, count: Int): java.util.Map[String, Object] = {
    val m = new java.util.LinkedHashMap[String, Object]()
    m.put("name", name)
    m.put("count", Int.box(count))
    m
  }

  private def assignmentMap(name: String, count: Int): java.util.Map[String, Object] = {
    val m = new java.util.LinkedHashMap[String, Object]()
    m.put("name", name)
    m.put("count", Int.box(count))
    m
  }

  /**
   * Build a Kueue Workload as a GenericKubernetesResource with a driver PodSet (count=1) and an
   * executor PodSet at the given spec count, and an optional admitted executor count.
   */
  private def buildWorkload(
      name: String = "spark-app",
      executorSpecCount: Int = 0,
      admittedExecutorCount: Option[Int] = None,
      targetExecutorCount: Option[Int] = None,
      observedGeneration: Option[Long] = None,
      generation: Option[Long] = Some(1L),
      finished: Boolean = false): GenericKubernetesResource = {
    val metadata = new ObjectMetaBuilder().withName(name).withNamespace("default").build()
    generation.foreach(g => metadata.setGeneration(g))
    val wl = new GenericKubernetesResource()
    wl.setMetadata(metadata)

    val podSets = new java.util.ArrayList[Object]()
    podSets.add(podSetMap(driverPodSet, 1))
    podSets.add(podSetMap(executorPodSet, executorSpecCount))
    val spec = new java.util.LinkedHashMap[String, Object]()
    spec.put("podSets", podSets)
    wl.setAdditionalProperty("spec", spec)

    val status = new java.util.LinkedHashMap[String, Object]()
    admittedExecutorCount.foreach { a =>
      val assignments = new java.util.ArrayList[Object]()
      assignments.add(assignmentMap(driverPodSet, 1))
      val execAssignment = assignmentMap(executorPodSet, a)
      // Kueue sets reclaimTargetCount on the executor assignment to request a partial-preemption
      // scale-down (below the admitted count).
      targetExecutorCount.foreach(e => execAssignment.put("reclaimTargetCount", Int.box(e)))
      assignments.add(execAssignment)
      val admission = new java.util.LinkedHashMap[String, Object]()
      admission.put("podSetAssignments", assignments)
      status.put("admission", admission)
    }
    val conditions = new java.util.ArrayList[Object]()
    observedGeneration.foreach { generation =>
      val condition = new java.util.LinkedHashMap[String, Object]()
      condition.put("type", "UnrelatedCondition")
      condition.put("status", "True")
      condition.put("observedGeneration", Long.box(generation))
      conditions.add(condition)
    }
    if (finished) {
      val cond = new java.util.LinkedHashMap[String, Object]()
      cond.put("type", "Finished")
      cond.put("status", "True")
      conditions.add(cond)
    }
    if (!conditions.isEmpty) {
      status.put("conditions", conditions)
    }
    if (!status.isEmpty) {
      wl.setAdditionalProperty("status", status)
    }
    wl
  }

  /** Read the podSets from a captured spec JSON merge patch. */
  private def patchedPodSets(json: String): Map[String, java.util.Map[String, Object]] = {
    val res = serialization.unmarshal(
      new ByteArrayInputStream(json.getBytes(UTF_8)), classOf[GenericKubernetesResource])
    val spec = res.getAdditionalProperties.get("spec").asInstanceOf[java.util.Map[String, Object]]
    spec.get("podSets").asInstanceOf[java.util.List[_]].asScala.map { ps =>
      val m = ps.asInstanceOf[java.util.Map[String, Object]]
      m.get("name").toString -> m
    }.toMap
  }

  private def executorPodAnswer(): Answer[KubernetesExecutorSpec] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      KubernetesExecutorSpec(executorPodWithId(0, k8sConf.resourceProfileId.toInt), Seq.empty)
    }

  before {
    MockitoAnnotations.openMocks(this).close()
    conf = new SparkConf(false)
      .set("spark.app.id", TEST_SPARK_APP_ID)
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
      .set(KUBERNETES_KUEUE_RESIZE_UP_INTERVAL, 0L)
      .set(KUBERNETES_KUEUE_RESIZE_UP_MIN_INCREMENT, 1)
      .set(KUBERNETES_DRIVER_OWN_PVC, false)
      .set(KUBERNETES_DRIVER_REUSE_PVC, false)
    secMgr = new SecurityManager(conf)
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()

    // Generic (Kueue Workload) DSL chain.
    when(kubernetesClient.getKubernetesSerialization).thenReturn(serialization)
    when(kubernetesClient.genericKubernetesResources(any(classOf[ResourceDefinitionContext])))
      .thenReturn(genericMixed)
    when(genericMixed.inNamespace("default")).thenReturn(genericNamespaced)
    when(genericNamespaced.withLabel(meq(jobUidLabel), anyString)).thenReturn(genericFiltered)
    when(genericFiltered.watch(any())).thenReturn(watch)
    when(genericNamespaced.resource(any(classOf[GenericRes]))).thenReturn(genericResource)
    when(genericResource.patch(any(classOf[PatchContext]), anyString))
      .thenReturn(null)

    // Pod DSL chain so the base allocator's onNewSnapshots can run without NPEs.
    when(kubernetesClient.pods()).thenReturn(pods)
    when(pods.inNamespace("default")).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(driverPodName)).thenReturn(driverPodResource)
    when(podsWithNamespace.resource(any(classOf[Pod]))).thenReturn(execPodResource)
    when(driverPodResource.waitUntilReady(any(), any())).thenReturn(driverPodWithController(true))
    when(execPodResource.create())
      .thenReturn(new PodBuilder().withNewMetadata().withName("exec").endMetadata().build())
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile]))).thenAnswer(executorPodAnswer())
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty)
  }

  /** Test subclass exposing the (protected) capped executor target for assertions. */
  private class TestKueueExecutorPodsAllocator
    extends KueueExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, new ManualClock()) {
    def executorTarget(rpId: Int): Option[Int] =
      Option(totalExpectedExecutorsPerResourceProfileId.get(rpId)).map(_.intValue())
  }

  private def newAllocator(): TestKueueExecutorPodsAllocator =
    new TestKueueExecutorPodsAllocator()

  private def seedWorkloads(items: GenericKubernetesResource*): Unit = {
    when(genericFiltered.list()).thenReturn(genericList)
    when(genericList.getItems).thenReturn(items.toList.asJava)
  }

  private def defaultProfile(): ResourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf)

  test("start fails when no Workload is found for the job-uid label") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads()

    val allocator = newAllocator()
    val e = intercept[SparkException] {
      allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    }
    assert(e.getMessage.contains("No Kueue Workload found"))
  }

  test("start locates the Workload using the driver pod's controlling owner UID") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload())

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)

    val labelValue = ArgumentCaptor.forClass(classOf[String])
    verify(genericNamespaced, org.mockito.Mockito.atLeastOnce())
      .withLabel(meq(jobUidLabel), labelValue.capture())
    assert(labelValue.getValue === appUid)
  }

  test("start fails when the driver pod has no controlling owner reference") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(false))
    seedWorkloads(buildWorkload())

    val allocator = newAllocator()
    val e = intercept[SparkException] {
      allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    }
    assert(e.getMessage.contains("no controlling owner reference"))
  }

  test("a finished Workload is ignored") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload(finished = true))

    val allocator = newAllocator()
    val e = intercept[SparkException] {
      allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    }
    assert(e.getMessage.contains("No Kueue Workload found"))
  }

  test("reconcile patches executor PodSet count to the target and leaves driver PodSet untouched") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload(executorSpecCount = 0))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 2))

    val patchJson = ArgumentCaptor.forClass(classOf[String])
    verify(genericResource).patch(any(classOf[PatchContext]), patchJson.capture())
    val podSets = patchedPodSets(patchJson.getValue)
    assert(podSets(executorPodSet).get("count") === 2)
    // Driver PodSet preserved at its original count and not mutated.
    assert(podSets(driverPodSet).get("count") === 1)
  }

  test("normal resize waits for a newer Kueue condition before patching again") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    val initial = buildWorkload(
      executorSpecCount = 2,
      admittedExecutorCount = Some(2),
      observedGeneration = Some(1),
      generation = Some(1))
    val pending = buildWorkload(
      executorSpecCount = 4,
      admittedExecutorCount = Some(2),
      observedGeneration = Some(1),
      generation = Some(2))
    when(genericResource.patch(any(classOf[PatchContext]), anyString)).thenReturn(pending)
    seedWorkloads(initial)

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 4))
    verify(genericResource).patch(any(classOf[PatchContext]), anyString)

    val patchCount = org.mockito.Mockito.mockingDetails(genericResource).getInvocations.asScala
      .count(_.getMethod.getName == "patch")
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 5))
    val blockedPatchCount = org.mockito.Mockito.mockingDetails(genericResource)
      .getInvocations.asScala.count(_.getMethod.getName == "patch")
    assert(blockedPatchCount === patchCount)

    val acknowledged = buildWorkload(
      executorSpecCount = 4,
      admittedExecutorCount = Some(4),
      observedGeneration = Some(2),
      generation = Some(2))
    // Simulate the MODIFIED event that refreshes the allocator cache.
    val watcher = ArgumentCaptor.forClass(classOf[io.fabric8.kubernetes.client.Watcher[GenericRes]])
    verify(genericFiltered).watch(watcher.capture())
    watcher.getValue.eventReceived(io.fabric8.kubernetes.client.Watcher.Action.MODIFIED,
      acknowledged)
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 5))
    val finalPatchCount = org.mockito.Mockito.mockingDetails(genericResource).getInvocations.asScala
      .count(_.getMethod.getName == "patch")
    assert(finalPatchCount === patchCount + 1)
  }

  test("reconcile skips the patch when spec already matches the desired count") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload(executorSpecCount = 2))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 2))

    verify(genericResource, never()).patch(any(classOf[PatchContext]), anyString)
  }

  test("executor pod target is capped at min(target, admitted)") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    // target = 5 desired, but Kueue has only admitted 3.
    seedWorkloads(buildWorkload(executorSpecCount = 5, admittedExecutorCount = Some(3)))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 5))

    assert(allocator.executorTarget(DEFAULT_RESOURCE_PROFILE_ID) === Some(3))
  }

  test(
    "scale-down floors the patched count at the resource-occupying pod count (no early release)") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload(executorSpecCount = 5, admittedExecutorCount = Some(5)))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    // Dynamic-allocation target dropped to 1, but 4 executor pods still occupy resources per the
    // API-server snapshot.
    snapshotsStore.replaceSnapshot(
      Seq(runningExecutor(1), runningExecutor(2), runningExecutor(3), runningExecutor(4)))
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 1))

    // The patched count is floored at occupying=4 (not the dynamic-allocation target 1), so Kueue
    // keeps quota for the pods still occupying resources; it only follows down as those pods are
    // actually removed.
    val patchJson = ArgumentCaptor.forClass(classOf[String])
    verify(genericResource).patch(any(classOf[PatchContext]), patchJson.capture())
    assert(patchedPodSets(patchJson.getValue)(executorPodSet).get("count") === 4)
  }

  test("partial preemption: reclaimTargetCount caps the executor target below the admitted count") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    // Admitted 5, but Kueue requests a partial-preemption scale-down to 2 via reclaimTargetCount.
    // specCount is already 2 (the count patch is exercised separately below) so this test
    // isolates the effective-target cap without triggering a spec patch.
    seedWorkloads(buildWorkload(
      executorSpecCount = 2, admittedExecutorCount = Some(5), targetExecutorCount = Some(2)))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    // Dynamic allocation still wants 5, but the effective target is min(5, admitted 5, expected 2).
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 5))

    assert(allocator.executorTarget(DEFAULT_RESOURCE_PROFILE_ID) === Some(2))
  }

  test("partial preemption: patched count follows reclaimTargetCount down, floored at occupying") {
    when(driverPodResource.get()).thenReturn(driverPodWithController(true))
    seedWorkloads(buildWorkload(
      executorSpecCount = 5, admittedExecutorCount = Some(5), targetExecutorCount = Some(2)))

    val allocator = newAllocator()
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
    // 3 executor pods still occupy resources while shedding towards reclaimTargetCount=2.
    snapshotsStore.replaceSnapshot(
      Seq(runningExecutor(1), runningExecutor(2), runningExecutor(3)))
    allocator.setTotalExpectedExecutors(Map(defaultProfile() -> 5))

    // requestedTarget = min(DA 5, expected 2) = 2, floored at occupying 3 => patch count = 3
    // (heading down to 2 as the remaining pods terminate; quota released only as pods leave).
    val patchJson = ArgumentCaptor.forClass(classOf[String])
    verify(genericResource).patch(any(classOf[PatchContext]), patchJson.capture())
    assert(patchedPodSets(patchJson.getValue)(executorPodSet).get("count") === 3)
  }
}
