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

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.client.{KubernetesClient, Watcher, WatcherException}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.dsl.base.{PatchContext, PatchType, ResourceDefinitionContext}

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.{Clock, Utils}

/**
 * Kubernetes executor allocator that uses Kueue in-place Workload resize
 * (ElasticJobsViaWorkloadResize) as the admission protocol for Spark executors.
 *
 * A SparkApplication corresponds to a single Kueue Workload, created and admitted by the Kueue
 * SparkApplication integration before the driver pod is started; that Workload has two PodSets,
 * `driver` and `executor`. This allocator does not create, delete, or otherwise own the Workload.
 * It:
 *   - locates the (single) Workload for this application by the `kueue.x-k8s.io/job-uid` label,
 *     whose value is the owning SparkApplication's UID;
 *   - on each reconcile, patches the `executor` PodSet's `spec.podSets[].count` to the target
 *     executor count computed by Spark dynamic allocation;
 *   - reads the admitted executor count from `status.admission` and caps the number of executor
 *     pods it creates to min(target count, admitted count);
 *   - honors `status.admission.podSetAssignments[].reclaimTargetCount` when Kueue requests a
 *     partial-preemption scale-down: it caps the executor request at reclaimTargetCount, sheds
 *     the excess running executors down to reclaimTargetCount (graceful decommission when
 *     spark.decommission.enabled is set, else force-kill) so a busy job releases the quota
 *     instead of waiting for an idle-timeout that never fires, and lowers the count it asks
 *     Kueue for towards reclaimTargetCount (floored at the occupying count), so the borrowed
 *     quota is given back as the shed pods actually terminate.
 *
 * Kueue is the sole writer of the Workload status (the admitted and expected counts); Spark only
 * writes `spec.podSets[].count`. Whether a scale-up is needed is decided by (target count >
 * admitted count), handled on the Kueue side.
 *
 * Workload state is read from a local cache maintained by a label-scoped watch to avoid listing on
 * every snapshot.
 */
private[spark] class KueueExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock)
  extends ExecutorPodsAllocator(conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore,
    clock) with Logging {

  import KueueExecutorPodsAllocator._

  private val desiredExecutorsPerResourceProfileId = new ConcurrentHashMap[Int, Int]()

  // Last effective per-ResourceProfile executor targets applied (min of target and admitted
  // counts); used to log admission changes at INFO without per-snapshot spam. Only touched from
  // the (single-threaded) snapshot subscriber callback.
  private var lastEffectiveTargets: Map[Int, Int] = Map.empty

  // Timestamp of the most recent normal scale-up request.
  private var lastScaleUpAtMillis: Option[Long] = None

  // Executor ids we have already asked to shed (decommission or force-kill) for partial preemption.
  // Prevents re-issuing the request for the same executor on every snapshot while it drains/exits.
  private val shedRequested = ConcurrentHashMap.newKeySet[String]()

  private val workloadClient = new KueueWorkloadClient(
    kubernetesClient,
    namespace,
    conf.get(KUBERNETES_KUEUE_WORKLOAD_API_VERSION))

  // UID of the owning SparkApplication; the Workload for this app is labeled job-uid=<this>.
  @volatile private var sparkApplicationUid: String = _
  @volatile private var workloadWatchConnection: Closeable = _

  override def start(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    super.start(applicationId, schedulerBackend)
    sparkApplicationUid = resolveSparkApplicationUid()
    startWorkloadWatch()
    if (workloadClient.cachedWorkload().isEmpty) {
      throw new SparkException(
        s"No Kueue Workload found with label $JobUidLabel=$sparkApplicationUid. The Kueue " +
          "SparkApplication integration is expected to have created and admitted the Workload " +
          "before the driver starts.")
    }
  }

  override def setTotalExpectedExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      desiredExecutorsPerResourceProfileId.put(rp.id, numExecs)
    }
    logDebug(s"Set Kueue desired execs to $desiredExecutorsPerResourceProfileId")
    snapshotsStore.notifySubscribers()
  }

  override protected def onNewSnapshots(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    Utils.tryLogNonFatalError {
      val desired = desiredCountsSnapshot()
      val occupying = quotaOccupyingExecutorCounts(snapshots)
      if (desired.nonEmpty) {
        reconcileExecutorPodSet(desired, occupying)
      }
      applyAdmittedExecutorTargets(desired)
      shedExcessExecutors(schedulerBackend)
    }
    super.onNewSnapshots(applicationId, schedulerBackend, snapshots)
  }

  override def stop(applicationId: String): Unit = {
    stopWorkloadWatch()
    super.stop(applicationId)
  }

  /**
   * Force a partial-preemption scale-down: when Kueue sets a reclaimTargetCount below the number of
   * currently running executors, shed the excess down to reclaimTargetCount instead of relying on
   * dynamic-allocation idle-timeout - which never fires while the executors are busy, so a loaded
   * job would otherwise never release the reclaimed quota.
   *
   * When graceful decommission is enabled (spark.decommission.enabled), the excess executors are
   * decommissioned: each finishes its running tasks and stops taking new ones before exiting (no
   * task loss; shuffle is served by the remote shuffle service on K8s). When it is disabled they
   * are force-killed instead, so the borrowed quota is still released - we must not hold onto it
   * just because decommission is off. Either way, once the pods terminate occupancy drops and
   * reconcileExecutorPodSet lowers spec.count so Kueue releases the quota.
   *
   * Scoped to the default ResourceProfile (the single Spark executor PodSet). Idempotent: executors
   * already asked to shed are tracked so we do not re-issue every snapshot.
   */
  private def shedExcessExecutors(
      schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    val wl = workloadClient.cachedWorkload().getOrElse(return)
    val reclaimTargetCount = workloadClient.reclaimTargetCount(wl, ExecutorPodSetName) match {
      case Some(tc) => tc
      case None =>
        // No partial-preemption request in effect; drop stale bookkeeping and do nothing.
        shedRequested.clear()
        return
    }
    val active = schedulerBackend.getExecutorIds().toSet
    // Forget executors that have already exited so the set stays bounded.
    shedRequested.retainAll(active.asJava)
    // Executors that would remain running (not already being shed).
    val remaining = active.diff(shedRequested.asScala.toSet)
    val excess = remaining.size - reclaimTargetCount
    if (excess <= 0) {
      return
    }
    // Shed the newest executors first (highest numeric id) for deterministic behavior.
    val toShed = remaining.toSeq
      .sortBy(id => id.toLongOption.getOrElse(0L))(Ordering[Long].reverse)
      .take(excess)
    // adjustTargetNumExecutors=false in both paths: applyAdmittedExecutorTargets already caps the
    // request at reclaimTargetCount, so no replacement pods are created; we must not also lower the
    // backend's target.
    if (conf.get(KUBERNETES_KUEUE_FORCE_KILL_ON_RECLAIM) || !conf.get(DECOMMISSION_ENABLED)) {
      // Force-kill so the borrowed quota is released promptly.
      schedulerBackend.killExecutors(toShed, adjustTargetNumExecutors = false,
        countFailures = false, force = true)
      logInfo(s"Kueue partial preemption: force-killed ${toShed.size} " +
        s"executor(s) [${toShed.mkString(",")}] to reach reclaimTargetCount=$reclaimTargetCount")
    } else {
      val infos = toShed.map { id =>
        id -> ExecutorDecommissionInfo(
          s"Kueue partial preemption: scaling executors down to $reclaimTargetCount")
      }.toArray
      schedulerBackend.decommissionExecutors(infos, adjustTargetNumExecutors = false,
        triggeredByExecutor = false)
      logInfo(s"Kueue partial preemption: requested decommission of ${toShed.size} " +
        s"executor(s) [${toShed.mkString(",")}] to reach reclaimTargetCount=$reclaimTargetCount")
    }
    shedRequested.addAll(toShed.asJava)
  }

  /**
   * Patch the `executor` PodSet's count on the single Workload to match the desired executor count.
   * Both scale-up and scale-down are an in-place count patch; the driver PodSet, all templates, and
   * the executor PodSet's minCount are preserved untouched (this allocator never reads or writes
   * minCount - it is a fixed job property owned by the Kueue integration).
   *
   * On scale-down the patched count is floored at the number of executor pods still occupying
   * resources (`occupying`): Spark dynamic allocation lowers its target immediately when the
   * backlog drains, but only tears the idle executors down later (after `executorIdleTimeout`).
   * Patching straight to the low target would make Kueue converge the admission and release the
   * quota while those executor pods are still occupying resources - a transient over-subscription
   * window. Flooring the patched count at the occupying count makes Kueue release quota only as
   * pods actually stop occupying resources (the teardown is driven by dynamic allocation
   * independently of this count, so the occupying count keeps dropping and the patched count
   * follows it down).
   */
  private def reconcileExecutorPodSet(desired: Map[Int, Int], occupying: Map[Int, Int]): Unit = {
    val wl = workloadClient.cachedWorkload().getOrElse {
      throw new SparkException(
        s"Kueue Workload with label $JobUidLabel=$sparkApplicationUid is no longer present.")
    }

    val target = desired.getOrElse(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, 0)
    val occupyingExecutors = occupying.getOrElse(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, 0)
    val admittedExecutors = workloadClient.admittedCount(wl, ExecutorPodSetName).getOrElse(0)
    val targetPreemptExecutors = workloadClient.reclaimTargetCount(wl, ExecutorPodSetName)
    val currentSpec = workloadClient.specCount(wl, ExecutorPodSetName).getOrElse(0)
    val requestedTarget = targetPreemptExecutors.map(math.min(target, _)).getOrElse(target)
    val desiredExecutors = math.max(requestedTarget, occupyingExecutors)

    if (currentSpec == desiredExecutors) {
      logDebug(s"Skipping Kueue Workload ${workloadKey(wl)} resize: " +
        s"spec=$currentSpec already matches desired=$desiredExecutors, " +
        s"target=$target, occupying=$occupyingExecutors, admitted=$admittedExecutors, " +
        s"reclaimTarget=${targetPreemptExecutors.getOrElse("-")}")
      return
    }

    val scaleUp = desiredExecutors > currentSpec
    val currentGeneration = workloadClient.generation(wl).getOrElse(-1L)
    val observedGeneration = workloadClient.maxObservedGeneration(wl)
    if (scaleUp && observedGeneration.exists(_ < currentGeneration)) {
      logDebug(s"Skipping Kueue Workload ${workloadKey(wl)} scale-up: " +
        s"generation=$currentGeneration is not acknowledged by " +
        s"observedGeneration=$observedGeneration, current=$currentSpec, " +
        s"desired=$desiredExecutors")
      return
    }

    val interval = conf.get(KUBERNETES_KUEUE_RESIZE_UP_INTERVAL)
    val minIncrement = conf.get(KUBERNETES_KUEUE_RESIZE_UP_MIN_INCREMENT)
    val now = clock.getTimeMillis()
    val intervalElapsed = lastScaleUpAtMillis.forall(now - _ >= interval)
    val incrementReached = desiredExecutors - currentSpec >= minIncrement
    if (scaleUp && !intervalElapsed && !incrementReached) {
      logDebug(s"Skipping Kueue Workload ${workloadKey(wl)} scale-up: " +
        s"both limits are unmet, current=$currentSpec, desired=$desiredExecutors, " +
        s"increment=${desiredExecutors - currentSpec}, minIncrement=$minIncrement, " +
        s"lastScaleUpAt=${lastScaleUpAtMillis.getOrElse("never")}, interval=$interval")
      return
    }

    val patched = workloadClient.patchExecutorPodSetCount(wl, ExecutorPodSetName, desiredExecutors)
    if (scaleUp) {
      lastScaleUpAtMillis = Some(clock.getTimeMillis())
    }
    logInfo(s"Patched Kueue Workload ${workloadKey(wl)} executor PodSet to count=" +
      s"$desiredExecutors (target=$target, occupying=$occupyingExecutors, " +
      s"admitted=$admittedExecutors, reclaimTarget=${targetPreemptExecutors.getOrElse("-")}, " +
      s"generation=${workloadClient.generation(patched).getOrElse("unknown")})")
  }

  private def applyAdmittedExecutorTargets(desired: Map[Int, Int]): Unit = {
    val admitted = admittedCountsSnapshot()
    val reclaimTarget = reclaimTargetCountsSnapshot()
    val capped = desired.map { case (rpId, count) =>
      val byAdmitted = math.min(count, admitted.getOrElse(rpId, 0))
      // reclaimTargetCount, when Kueue requests a partial-preemption scale-down, further caps the
      // number of executors we run (below the admitted count) so idle-timeout teardown starts
      // shedding immediately. Absent => no extra cap.
      val effective = reclaimTarget.get(rpId).map(math.min(byAdmitted, _)).getOrElse(byAdmitted)
      rpId -> effective
    }

    totalExpectedExecutorsPerResourceProfileId.keySet().asScala
      .filterNot(capped.contains)
      .foreach(totalExpectedExecutorsPerResourceProfileId.remove)
    capped.foreach { case (rpId, count) =>
      totalExpectedExecutorsPerResourceProfileId.put(rpId, count)
    }

    if (capped != lastEffectiveTargets) {
      logInfo(s"Kueue executor admission updated: desired=$desired, admitted=$admitted, " +
        s"reclaimTarget=$reclaimTarget, startingExecutors=$capped")
      lastEffectiveTargets = capped
    }
  }

  private def startWorkloadWatch(): Unit = {
    require(workloadWatchConnection == null, "Cannot start the Kueue workload watcher twice.")
    workloadWatchConnection = workloadClient.startCacheWatch(sparkApplicationUid) {
      snapshotsStore.notifySubscribers()
    }
  }

  private def stopWorkloadWatch(): Unit = {
    if (workloadWatchConnection != null) {
      Utils.tryLogNonFatalError {
        workloadWatchConnection.close()
      }
      workloadWatchConnection = null
    }
  }

  // The admitted executor count, keyed by the default ResourceProfile
  // id (single executor PodSet).
  private def admittedCountsSnapshot(): Map[Int, Int] = {
    workloadClient.cachedWorkload().flatMap { wl =>
      workloadClient.admittedCount(wl, ExecutorPodSetName)
        .map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> _)
    }.toMap
  }

  /**
   * The Kueue-requested partial-preemption "scale down to" executor count, keyed by the default
   * ResourceProfile id. Present only when Kueue has requested a partial preemption
   * (reclaimTargetCount set on the executor PodSet's admission); empty otherwise.
   */
  private def reclaimTargetCountsSnapshot(): Map[Int, Int] = {
    workloadClient.cachedWorkload().flatMap { wl =>
      workloadClient.reclaimTargetCount(wl, ExecutorPodSetName)
        .map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> _)
    }.toMap
  }

  private def desiredCountsSnapshot(): Map[Int, Int] = {
    desiredExecutorsPerResourceProfileId.asScala.toMap
  }

  /**
   * Executor pods that currently occupy resources/quota, from the latest API-server-derived
   * snapshot (`ExecutorPodsSnapshot`), keyed by the default ResourceProfile id (single executor
   * PodSet). Used as the scale-down floor for the target count so Kueue releases quota only as pods
   * actually stop occupying resources - a killed executor keeps counting until the API server has
   * removed its pod.
   *
   * Occupying (counted): `PodRunning` / `PodTerminating` (holding resources now),
   * `PodPending` (will hold once scheduled), `PodUnknown` (assume occupying).
   * Not occupying (excluded): `PodSucceeded` / `PodFailed` (finished), `PodDeleted` (removed).
   */
  private def quotaOccupyingExecutorCounts(
      snapshots: Seq[ExecutorPodsSnapshot]): Map[Int, Int] = {
    val current = snapshots.lastOption.getOrElse(lastSnapshot)
    val occupying = current.executorPods.values.count {
      case PodSucceeded(_) | PodFailed(_) | PodDeleted(_) => false
      case _ => true
    }
    if (occupying > 0) Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> occupying) else Map.empty
  }

  /**
   * Resolve the owning job's UID from the driver pod's controlling owner reference. Kueue labels
   * the Workload `job-uid=<owning object UID>`; under Spark Operator the driver pod's controller
   * owner is exactly that object, so we read its UID without hardcoding the owner's CRD kind.
   */
  private def resolveSparkApplicationUid(): String = {
    val pod = driverPod.getOrElse {
      throw new SparkException(
        "A driver pod is required to locate the Kueue Workload; set " +
          "spark.kubernetes.driver.pod.name (this allocator must run under Spark Operator).")
    }
    Option(pod.getMetadata)
      .flatMap(m => Option(m.getOwnerReferences))
      .map(_.asScala)
      .getOrElse(Nil)
      .find(ref => Option(ref.getController).exists(_.booleanValue()))
      .map(_.getUid)
      .getOrElse {
        throw new SparkException(
          s"The driver pod ${pod.getMetadata.getName} has no controlling owner reference; the " +
            "Kueue in-place resize allocator requires running under Spark Operator.")
      }
  }

  private def workloadKey(workload: GenericKubernetesResource): String = {
    s"${workload.getMetadata.getNamespace}/${workload.getMetadata.getName}"
  }
}

private[spark] object KueueExecutorPodsAllocator {
  private val KueueApiGroup = "kueue.x-k8s.io"
  private val KueueWorkloadKind = "Workload"
  private val KueueWorkloadPlural = "workloads"

  // Label Kueue sets on the Workload, holding the owning job's UID.
  private val JobUidLabel = "kueue.x-k8s.io/job-uid"
  // PodSet name used by the Kueue SparkApplication integration for executors.
  private val ExecutorPodSetName = "executor"

  private class KueueWorkloadClient(
      kubernetesClient: KubernetesClient,
      namespace: String,
      apiVersion: String) extends Logging {

    private val serialization = kubernetesClient.getKubernetesSerialization

    private val workloadContext = new ResourceDefinitionContext.Builder()
      .withGroup(KueueApiGroup)
      .withVersion(apiVersion)
      .withKind(KueueWorkloadKind)
      .withPlural(KueueWorkloadPlural)
      .withNamespaced(true)
      .build()

    // Local cache of the (single) Workload for this app, keyed by name. Seeded once with a list,
    // then kept up to date by a label-scoped watch.
    private val cache = new ConcurrentHashMap[String, GenericKubernetesResource]()

    private def workloads = kubernetesClient.genericKubernetesResources(workloadContext)
      .inNamespace(namespace)

    /** The single non-finished Workload for this app, if present in the cache. */
    def cachedWorkload(): Option[GenericKubernetesResource] = {
      cache.values().asScala.filterNot(isFinished).toSeq.headOption
    }

    /** Seed the cache with a one-time list, then start a watch to keep it updated. */
    def startCacheWatch(jobUid: String)(onChange: => Unit): Closeable = {
      workloads
        .withLabel(JobUidLabel, jobUid)
        .list()
        .getItems
        .asScala
        .foreach(w => cache.put(w.getMetadata.getName, w))

      workloads
        .withLabel(JobUidLabel, jobUid)
        .watch(new Watcher[GenericKubernetesResource] {
          override def eventReceived(action: Action, resource: GenericKubernetesResource): Unit = {
            val name = Option(resource.getMetadata).map(_.getName).getOrElse("<unknown>")
            action match {
              case Action.ADDED | Action.MODIFIED => cache.put(name, resource)
              case Action.DELETED => cache.remove(name)
              case _ =>
            }
            logDebug(s"Received Kueue Workload event $action for $namespace/$name")
            onChange
          }

          override def onClose(e: WatcherException): Unit = {
            if (SparkContext.getActive.map(_.isStopped).getOrElse(true)) {
              logInfo("Kueue workload watch has been closed.")
            } else {
              logWarning("Kueue workload watch has been closed.", e)
            }
          }

          override def onClose(): Unit = {
            if (SparkContext.getActive.map(_.isStopped).getOrElse(true)) {
              logInfo("Kueue workload watch has been closed.")
            } else {
              logWarning("Kueue workload watch has been closed.")
            }
          }
        })
    }

    def patchExecutorPodSetCount(
        workload: GenericKubernetesResource,
        podSetName: String,
        count: Int): GenericKubernetesResource = {
      val existingPodSets = mapProperty(workload, "spec")
        .flatMap(spec => listProperty(spec, "podSets"))
        .getOrElse(Nil)
      val patchedPodSets = existingPodSets.map { ps =>
        asMap(ps).map { m =>
          val copy = new java.util.LinkedHashMap[String, Object](m)
          if (stringProperty(copy, "name").contains(podSetName)) {
            copy.put("count", Int.box(count))
          }
          copy: Object
        }.getOrElse(ps)
      }
      val specPatch = new java.util.LinkedHashMap[String, Object]
      specPatch.put("podSets", patchedPodSets.asJava)
      val patch = new java.util.LinkedHashMap[String, Object]
      patch.put("spec", specPatch)

      val patchJson = serialization.asJson(patch)
      val patched = workloads.resource(workload).patch(PatchContext.of(PatchType.JSON_MERGE),
        patchJson)
      // Refresh the cache with the patched object so a subsequent reconcile does not re-patch
      // stale spec counts before the watch delivers the MODIFIED event.
      Option(patched).foreach(w => cache.put(w.getMetadata.getName, w))
      Option(patched).getOrElse(workload)
    }

    /** Admitted count of the named PodSet from status.admission, if the Workload is admitted. */
    def admittedCount(workload: GenericKubernetesResource, podSetName: String): Option[Int] = {
      val assignments = mapProperty(workload, "status")
        .flatMap(status => mapProperty(status, "admission"))
        .flatMap(admission => listProperty(admission, "podSetAssignments"))
        .getOrElse(Nil)
      assignments.flatMap(asMap).find(a => stringProperty(a, "name").contains(podSetName))
        .flatMap(a => intProperty(a, "count")
          .orElse(specCount(workload, podSetName)))
    }

    /**
     * The Kueue-requested "scale down to" count for the named PodSet from
     * status.admission.podSetAssignments[].reclaimTargetCount, if set. Kueue sets this (below the
     * admitted count) to request a partial-preemption scale-down; it is absent when no
     * partial preemption is requested. The allocator uses it as an additional cap on the
     * number of executors it runs and on the count it asks Kueue for.
     */
    def reclaimTargetCount(workload: GenericKubernetesResource, podSetName: String): Option[Int] = {
      val assignments = mapProperty(workload, "status")
        .flatMap(status => mapProperty(status, "admission"))
        .flatMap(admission => listProperty(admission, "podSetAssignments"))
        .getOrElse(Nil)
      assignments.flatMap(asMap).find(a => stringProperty(a, "name").contains(podSetName))
        .flatMap(a => intProperty(a, "reclaimTargetCount"))
    }

    def specCount(workload: GenericKubernetesResource, podSetName: String): Option[Int] = {
      podSet(workload, podSetName).flatMap(ps => intProperty(ps, "count"))
    }

    def generation(workload: GenericKubernetesResource): Option[Long] = {
      Option(workload.getMetadata).flatMap(m => Option(m.getGeneration)).map(_.longValue())
    }

    /** Maximum observed generation across all Workload conditions. */
    def maxObservedGeneration(workload: GenericKubernetesResource): Option[Long] = {
      mapProperty(workload, "status")
        .flatMap(status => listProperty(status, "conditions"))
        .getOrElse(Nil)
        .flatMap(asMap)
        .flatMap(m => longProperty(m, "observedGeneration"))
        .reduceOption(_ max _)
    }

    private def podSet(
        workload: GenericKubernetesResource,
        podSetName: String): Option[java.util.Map[String, Object]] = {
      mapProperty(workload, "spec")
        .flatMap(spec => listProperty(spec, "podSets"))
        .getOrElse(Nil)
        .flatMap(asMap)
        .find(ps => stringProperty(ps, "name").contains(podSetName))
    }

    private def isFinished(workload: GenericKubernetesResource): Boolean = {
      mapProperty(workload, "status")
        .flatMap(status => listProperty(status, "conditions"))
        .getOrElse(Nil)
        .exists { condition =>
          asMap(condition).exists { conditionMap =>
            stringProperty(conditionMap, "type").contains("Finished") &&
              stringProperty(conditionMap, "status").contains("True")
          }
        }
    }

    private def mapProperty(
        workload: GenericKubernetesResource,
        key: String): Option[java.util.Map[String, Object]] = {
      asMap(workload.getAdditionalProperties.get(key))
    }

    private def mapProperty(
        map: java.util.Map[String, Object],
        key: String): Option[java.util.Map[String, Object]] = {
      asMap(map.get(key))
    }

    private def listProperty(
        map: java.util.Map[String, Object],
        key: String): Option[Seq[Object]] = {
      Option(map.get(key)).collect {
        case list: java.util.List[_] => list.asScala.map(_.asInstanceOf[Object]).toSeq
      }
    }

    private def stringProperty(
        map: java.util.Map[String, Object],
        key: String): Option[String] = {
      Option(map.get(key)).map(_.toString)
    }

    private def longProperty(
        map: java.util.Map[String, Object],
        key: String): Option[Long] = {
      Option(map.get(key)).flatMap {
        case n: java.lang.Number => Some(n.longValue())
        case s: String => scala.util.Try(s.toLong).toOption
        case _ => None
      }
    }

    private def intProperty(
        map: java.util.Map[String, Object],
        key: String): Option[Int] = {
      Option(map.get(key)).flatMap {
        case n: java.lang.Number => Some(n.intValue())
        case s: String => scala.util.Try(s.toInt).toOption
        case _ => None
      }
    }

    private def asMap(value: Object): Option[java.util.Map[String, Object]] = {
      Option(value).collect {
        case map: java.util.Map[_, _] => map.asInstanceOf[java.util.Map[String, Object]]
      }
    }
  }
}
