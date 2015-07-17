/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.fenzo;

import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsEvaluater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Action2;
import com.netflix.fenzo.functions.Func1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code TaskScheduler} provides a scheduling service for assigning resources to tasks. User calls the method
 * {@code scheduleOnce()} with a list of task requests and a list of new resource lease offers. Any unused lease
 * offers are stored for use in future calls to {@code scheduleOnce()} until a time transpires, defined by the
 * lease offer expiry time that is set when building the {@code TaskScheduler}. The default is 10 seconds.
 * Upon reaching the expiry time, resource lease offers are rejected by invoking the action supplied with the builder.
 * 
 * TaskScheduler can be used in two modes:
 * <dl>
 *  <dt>Simple mode with no optimizations</dt>
 *   <dd></dd>In this mode, after building the {@code TaskScheduler} object, only the
 *            {@code scheduleOnce()} method need be called.</dd>
 *  <dt>Optimizations mode</dt>
 *   <dd>In this mode, (@code TaskScheduler} attempts to optimize the placement of tasks on
 *             resources by using the optimization functions (To be added). This requires that the user not only call
 *             {@code scheduleOnce()} method but also the task assigner and task un-assigner actions available from
 *             the methods {@code getTaskAssigner()} and {@code getTaskUnAssigner()}. These actions make the
 *             {@code TaskScheduler} keep track of tasks already assigned. Tracked tasks are then made available to optimization
 *             functions.</dd>
 *  </dl>
 *
 * 
 *     The scheduler cannot be called concurrently. Task assignment proceeds in the order of the tasks received in
 * given list. Each task is checked against available resources until a match is found.
 * 
 *     The builder provides other methods to set autoscaling rules and fitness calculators, etc.
 * 
 *
 */
public class TaskScheduler {

    private static final int PARALLEL_SCHED_EVAL_MIN_BATCH_SIZE = 30;

    /**
     * @warn class description missing
     */
    public final static class Builder {

        private Action1<VirtualMachineLease> leaseRejectAction=null;
        private long leaseOfferExpirySecs=120;
        private VMTaskFitnessCalculator fitnessCalculator = new DefaultFitnessCalculator();
        private String autoScaleByAttributeName=null;
        private String autoScalerMapHostnameAttributeName=null;
        private String autoScaleDownBalancedByAttributeName =null;
        private List<AutoScaleRule> autoScaleRules=new ArrayList<>();
        private Func1<Double, Boolean> isFitnessGoodEnoughFunction = new Func1<Double, Boolean>() {
            @Override
            public Boolean call(Double f) {
                return f>1.0;
            }
        };
        private boolean disableShortfallEvaluation=false;
        private Map<String, ResAllocs> resAllocs=null;

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param leaseRejectAction
         * @return
         */
        public Builder withLeaseRejectAction(Action1<VirtualMachineLease> leaseRejectAction) {
            this.leaseRejectAction = leaseRejectAction;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param leaseOfferExpirySecs
         * @return
         */
        public Builder withLeaseOfferExpirySecs(long leaseOfferExpirySecs) {
            this.leaseOfferExpirySecs = leaseOfferExpirySecs;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param fitnessCalculator
         * @return
         */
        public Builder withFitnessCalculator(VMTaskFitnessCalculator fitnessCalculator) {
            this.fitnessCalculator = fitnessCalculator;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param name
         * @return
         */
        public Builder withAutoScaleByAttributeName(String name) {
            this.autoScaleByAttributeName = name;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param name
         * @return
         */
        public Builder withAutoScalerMapHostnameAttributeName(String name) {
            this.autoScalerMapHostnameAttributeName = name;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param name
         * @return
         */
        public Builder withAutoScaleDownBalancedByAttributeName(String name) {
            this.autoScaleDownBalancedByAttributeName = name;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param f
         * @return
         */
        public Builder withFitnessGoodEnoughFunction(Func1<Double, Boolean> f) {
            this.isFitnessGoodEnoughFunction = f;
            return this;
        }

        /**
         * @warn method description missing
         *
         * @return
         */
        public Builder disableShortfallEvaluation() {
            disableShortfallEvaluation = true;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param resAcllocs
         * @return
         */
        public Builder withInitialResAllocs(Map<String, ResAllocs> resAllocs) {
            this.resAllocs = resAllocs;
            return this;
        }

        /**
         * @warn method description missing
         * @warn parameter description missing
         *
         * @param rule
         * @return
         */
        public Builder withAutoScaleRule(AutoScaleRule rule) {
            if(autoScaleByAttributeName==null || autoScaleByAttributeName.isEmpty())
                throw new IllegalArgumentException("Auto scale by attribute name must be set before setting rules");
            if(rule.getMinIdleHostsToKeep()<1)
                throw new IllegalArgumentException("Min Idle must be >0");
            if(rule.getMinIdleHostsToKeep()>rule.getMaxIdleHostsToKeep())
                throw new IllegalArgumentException("Min Idle must be <= Max Idle hosts");
            this.autoScaleRules.add(rule);
            return this;
        }

        /**
         * @warn method description missing
         *
         * @return
         */
        public TaskScheduler build() {
            return new TaskScheduler(this);
        }
    }

    private static class EvalResult {
        List<TaskAssignmentResult> assignmentResults;
        TaskAssignmentResult result;
        int numAllocationTrials;
        Exception exception;

        private EvalResult(List<TaskAssignmentResult> assignmentResults, TaskAssignmentResult result, int numAllocationTrials, Exception e) {
            this.assignmentResults = assignmentResults;
            this.result = result;
            this.numAllocationTrials = numAllocationTrials;
            this.exception = e;
        }
    }

    private final AssignableVMs assignableVMs;
    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);
    private static final long purgeVMsIntervalSecs = 60;
    private long lastVMPurgeAt=System.currentTimeMillis();
    private final Builder builder;
    private final StateMonitor stateMonitor;
    private final AutoScaler autoScaler;
    private final int EXEC_SVC_THREADS=Runtime.getRuntime().availableProcessors();
    private final ExecutorService executorService = Executors.newFixedThreadPool(EXEC_SVC_THREADS);
    private final ResAllocsEvaluater resAllocsEvaluator;

    private TaskScheduler(Builder builder) {
        if(builder.leaseRejectAction ==null)
            throw new IllegalArgumentException("Lease reject action must be non-null");
        this.builder = builder;
        this.stateMonitor = new StateMonitor();
        TaskTracker taskTracker = new TaskTracker();
        resAllocsEvaluator = new ResAllocsEvaluater(taskTracker, builder.resAllocs);
        assignableVMs = new AssignableVMs(taskTracker, builder.leaseRejectAction,
                builder.leaseOfferExpirySecs, builder.autoScaleByAttributeName);
        if(builder.autoScaleByAttributeName != null && !builder.autoScaleByAttributeName.isEmpty()) {

            autoScaler = new AutoScaler(builder.autoScaleByAttributeName, builder.autoScalerMapHostnameAttributeName,
                    builder.autoScaleDownBalancedByAttributeName,
                    builder.autoScaleRules, assignableVMs, null,
                    builder.disableShortfallEvaluation, assignableVMs.getActiveVmGroups());
        }
        else {
            autoScaler=null;
        }
    }

    /**
     * Set the autoscale call back action. This action is called when it this scheduler determines that the
     * cluster needs to be scaled up or down.
     * @param callback The callback to invoke for autoscale actions.
     * @throws IllegalStateException If autoscaler wasn't setup.
     */
    public void setAutoscalerCallback(Action1<AutoScaleAction> callback) throws IllegalStateException {
        if(autoScaler==null)
            throw new IllegalStateException("No autoScaler setup");
        autoScaler.setCallback(callback);
    }

    private TaskAssignmentResult getSuccessfulResult(List<TaskAssignmentResult> results) {
        double bestFitness=0.0;
        TaskAssignmentResult bestResult=null;
        for(int r=results.size()-1; r>=0; r--) {
            // change to using fitness value from assignment result
            TaskAssignmentResult res = results.get(r);
            if(res!=null && res.isSuccessful()) {
                if(bestResult==null || res.getFitness()>bestFitness) {
                    bestFitness = res.getFitness();
                    bestResult = res;
                }
            }
        }
        return bestResult;
    }

    private boolean isGoodEnough(TaskAssignmentResult result) {
        return builder.isFitnessGoodEnoughFunction.call(result.getFitness());
    }

    /**
     * Get the current mapping of resource allocations registered
     * @return Current mapping of resource allocations
     */
    public Map<String, ResAllocs> getResAllocs() {
        return resAllocsEvaluator.getResAllocs();
    }

    /**
     * Add a new resource allocation, or replace an existing one of the same name.
     * @param resAllocs The new resource allocation to add.
     */
    public void addOrReplaceResAllocs(ResAllocs resAllocs) {
        resAllocsEvaluator.replaceResAllocs(resAllocs);
    }

    /**
     * Remove resource allocation associated with the given name.
     * @param groupName Name of the resource allocation to remove.
     */
    public void removeResAllocs(String groupName) {
        resAllocsEvaluator.remResAllocs(groupName);
    }

    /**
     * Get the currently registered autoscale rules.
     * @return Collection of autoscale rules currently registered.
     */
    public Collection<AutoScaleRule> getAutoScaleRules() {
        if(autoScaler==null)
            return Collections.emptyList();
        return autoScaler.getRules();
    }

    /**
     * Add a new autoscale rule. If a rule with the same name exists, it is replaced. This autoscale rule is used
     * next time the autoscale action is invoked.
     * @param rule The autoscale rule to add.
     */
    public void addOrReplaceAutoScaleRule(AutoScaleRule rule) {
        autoScaler.replaceRule(rule);
    }

    /**
     * Remove the autoscale rule associated with the given name.
     * @param ruleName Name of the autoscale rule to remove.
     */
    /**
     * @warn method description missing
     * @warn parameterdescription missing
     *
     * @param ruleName
     */
    public void removeAutoScaleRule(String ruleName) {
        autoScaler.removeRule(ruleName);
    }

    /**
     * Schedule given task requests using newly added resource leases in addition to previously unused leases.
     * This is the main scheduling method that attempts to assign resources to the given task requests. Resource
     * leases are associated with a host name. A host can have zero or more leases. Leases unused in this scheduling
     * run are stored for later use until they expire. Attempt to add a lease object with an Id equal to that of a
     * stored lease object is disallowed by throwing {@code IllegalStateException}.
     * Upon throwing this exception, if multiple leases were given in the {@code newLeases} argument,
     * the state of internally maintained list of unused leases is unknown - some of the leases may have been
     * successfully added.
     * 
     * Any expired leases are rejected before scheduling begins. Then, all leases of a host are combined to
     * determine total available resources on the host. Each task request, in the order that they appear in
     * the given list, is then tried for assignment against the available hosts until successful. For each
     * task, either a successful assignment result, or, the set of assignment failures, is returned.
     * 
     * After all assignments have been evaluated, a certain number of leases are rejected if they are unused and
     * their offer time is longer than lease expiration interval. This is to prevent hoarding of leases. If an
     * autoscaler was provided, autoscale evaluation is launched to run asynchronously, in which each autoscale rule
     * registered is run based on its policy.
     * 
     * @param requests List of requests to assign resources to, in the given order.
     * @param newLeases New resource leases for hosts to be used in addition to any previously ununsed leases.
     * @return SchedulingResult object that contains task assignment results map and other summaries.
     * @throws IllegalStateException If called concurrently or if an existing lease is added again.
     */
    public SchedulingResult scheduleOnce(
            List<? extends TaskRequest> requests,
            List<VirtualMachineLease> newLeases) throws IllegalStateException {
        try (AutoCloseable
                     ac = stateMonitor.enter()) {
            long start = System.currentTimeMillis();
            final SchedulingResult schedulingResult = doSchedule(requests, newLeases);
            if((lastVMPurgeAt + purgeVMsIntervalSecs*1000) < System.currentTimeMillis()) {
                lastVMPurgeAt = System.currentTimeMillis();
                logger.info("Purging inactive VMs");
                assignableVMs.purgeInactiveVMs();
            }
            schedulingResult.setRuntime(System.currentTimeMillis() - start);
            return schedulingResult;
        } catch (Exception e) {
            logger.error("Error with scheduling run: " + e.getMessage(), e);
            if(e instanceof IllegalStateException)
                throw (IllegalStateException)e;
            else {
                logger.warn("Unexpected exception: " + e.getMessage());
                return null;
            }
        }
    }

    private SchedulingResult doSchedule(
            List<? extends TaskRequest> requests,
            List<VirtualMachineLease> newLeases) {
        AtomicInteger rejectedCount = new AtomicInteger(assignableVMs.addLeases(newLeases));
        List<AssignableVirtualMachine> avms = assignableVMs.prepareAndGetOrderedVMs();
        final boolean hasResAllocs = resAllocsEvaluator.prepare();
        //logger.info("Got " + avms.size() + " AVMs to schedule on");
        int totalNumAllocations=0;
        Set<TaskRequest> failedTasksForAutoScaler = new HashSet<>(requests);
        Map<String, VMAssignmentResult> resultMap = new HashMap<>(avms.size());
        final SchedulingResult schedulingResult = new SchedulingResult(resultMap);
        if(!avms.isEmpty()) {
            for(final TaskRequest task: requests) {
                if(hasResAllocs) {
                    if(resAllocsEvaluator.taskGroupFailed(task.taskGroupName()))
                        continue;
                    final AssignmentFailure resAllocsFailure = resAllocsEvaluator.hasResAllocs(task);
                    if(resAllocsFailure != null) {
                        final List<TaskAssignmentResult> failures = Collections.singletonList(new TaskAssignmentResult(assignableVMs.getDummyVM(),
                                task, false, Collections.singletonList(resAllocsFailure), null, 0.0));
                        schedulingResult.addFailures(task, failures);
                        failedTasksForAutoScaler.remove(task); // don't scale up for resAllocs failures
                        continue;
                    }
                }
                final AssignmentFailure maxResourceFailure = assignableVMs.getFailedMaxResource(null, task);
                if(maxResourceFailure != null) {
                    final List<TaskAssignmentResult> failures = Collections.singletonList(new TaskAssignmentResult(assignableVMs.getDummyVM(), task, false,
                            Collections.singletonList(maxResourceFailure), null, 0.0));
                    schedulingResult.addFailures(task, failures);
                    continue;
                }
                // create batches of VMs to evaluate assignments concurrently across the batches
                final BlockingQueue<AssignableVirtualMachine> virtualMachines = new ArrayBlockingQueue<>(avms.size(), false, avms);
                int nThreads = (int)Math.ceil((double)avms.size()/ PARALLEL_SCHED_EVAL_MIN_BATCH_SIZE);
                List<Future<EvalResult>> futures = new ArrayList<>();
                for(int b=0; b<nThreads && b<EXEC_SVC_THREADS; b++) {
                    futures.add(executorService.submit(new Callable<EvalResult>() {
                        @Override
                        public EvalResult call() throws Exception {
                            return evalAssignments(task, virtualMachines);
                        }
                    }));
                }
                List<EvalResult> results = new ArrayList<>();
                List<TaskAssignmentResult> bestResults = new ArrayList<>();
                for(Future<EvalResult> f: futures) {
                    try {
                        EvalResult evalResult = f.get();
                        if(evalResult.exception!=null)
                            logger.error("Error during concurrent task assignment eval - " + evalResult.exception.getMessage(),
                                    evalResult.exception);
                        else {
                            results.add(evalResult);
                            bestResults.add(evalResult.result);
                            totalNumAllocations += evalResult.numAllocationTrials;
                        }
                    } catch (InterruptedException|ExecutionException e) {
                        logger.error("Unexpected during concurrent task assignment eval - " + e.getMessage(), e);
                    }
                }
                TaskAssignmentResult successfulResult = getSuccessfulResult(bestResults);
                List<TaskAssignmentResult> failures = new ArrayList<>();
                if(successfulResult == null) {
                    for(EvalResult er: results)
                        failures.addAll(er.assignmentResults);
                    schedulingResult.addFailures(task, failures);
                }
                else {
                    successfulResult.assignResult();
                    failedTasksForAutoScaler.remove(task);
                }
            }
        }
        List<VirtualMachineLease> idleResourcesList = new ArrayList<>();
        for(AssignableVirtualMachine avm: avms) {
            VMAssignmentResult assignmentResult = avm.resetAndGetSuccessfullyAssignedRequests();
            if(assignmentResult==null) {
                if(!avm.hasPreviouslyAssignedTasks())
                    idleResourcesList.add(avm.getCurrTotalLease());
            }
            else {
                resultMap.put(avm.getHostname(), assignmentResult);
            }
        }
        rejectedCount.addAndGet(assignableVMs.removeLimitedLeases(idleResourcesList));
        final AutoScalerInput autoScalerInput = new AutoScalerInput(idleResourcesList, failedTasksForAutoScaler);
        if(autoScaler!=null)
            autoScaler.scheduleAutoscale(autoScalerInput);
        schedulingResult.setLeasesAdded(newLeases.size());
        schedulingResult.setLeasesRejected(rejectedCount.get());
        schedulingResult.setNumAllocations(totalNumAllocations);
        schedulingResult.setTotalVMsCount(assignableVMs.getTotalNumVMs());
        schedulingResult.setIdleVMsCount(idleResourcesList.size());
        return schedulingResult;
    }

    /**
     * Returns state of resources on all known hosts. This is expected to be used for debugging or informational
     * purposes only, and occasionally at that. Calling this obtains and holds a lock for the duration of creating the
     * state information. Scheduling runs are blocked around the lock.
     * 
     * @return Map of state information with hostname as key and a Map of resource state. The resource state Map contains
     * resource as the key and a two element Double array - first contains used value and the second element contains
     * available value (available does not include used).
     */
    public Map<String, Map<VMResource, Double[]>> getResourceStatus() {
        try (AutoCloseable ac = stateMonitor.enter()) {
            return assignableVMs.getResourceStatus();
        } catch (Exception e) {
            logger.error("Unexpected error from state monitor: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns current state of all known hosts. This is expected to be used for debugging or informational
     * purposes only, and occasionally at that. Calling this obtains and holds a lock for the duration of creating the
     * state information. Scheduling runs are blocked around the lock.
     * 
     * @return List of current state of all known VMs
     * @throws IllegalStateException If called concurrently with main scheduling method, scheduleOnce().
     */
    public List<VirtualMachineCurrentState> getVmCurrentStates() throws IllegalStateException {
        try (AutoCloseable ac = stateMonitor.enter()) {
            return assignableVMs.getVmCurrentStates();
        }
        catch (Exception e) {
            logger.error("Unexpected error from state monitor: " + e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private EvalResult evalAssignments(TaskRequest task, BlockingQueue<AssignableVirtualMachine> virtualMachines) {
        // This number below sort of controls minimum machines to eval, choose carefully.
        // Having it too small increases overhead of getting next machine to evaluate on.
        // Having it too high increases latency of thread before it returns when done
        try {
            int N=10;
            List<AssignableVirtualMachine> buf = new ArrayList<>(N);
            List<TaskAssignmentResult> results = new ArrayList<>();
            while(true) {
                buf.clear();
                int n = virtualMachines.drainTo(buf, N);
                if(n == 0)
                    return new EvalResult(results, getSuccessfulResult(results), results.size(), null);
                for(int m=0; m<n; m++) {
                    TaskAssignmentResult result = buf.get(m).tryRequest(task, builder.fitnessCalculator);
                    results.add(result);
                    if(result.isSuccessful() && builder.isFitnessGoodEnoughFunction.call(result.getFitness())) {
                        // drain rest of the queue, nobody needs to do more work.
                        virtualMachines.clear();
                        // Instead of returning here, we finish computing on rest of the machines in buf
                    }
                }
            }
        }
        catch (Exception e) {
            return new EvalResult(null, null, 0, e);
        }
    }

    /**
     * Expire a resource lease with the given lease Id.
     * @param leaseId Lease ID of the lease to expire.
     */
    public void expireLease(String leaseId) {
        assignableVMs.expireLease(leaseId);
    }

    /**
     * Expire all leases of a host with the name <code>hostname</code>.
     * @param hostname Name of the host.
     */
    public void expireAllLeases(String hostname) {
        assignableVMs.expireAllLeases(hostname);
    }

    /**
     * Expire all leases of a host with the Id, <code>vmId</code>.
     * @param vmId ID of the host.
     * @return True if the given Id was known, false otherwise.
     */
    public boolean expireAllLeasesByVMId(String vmId) {
        final String hostname = assignableVMs.getHostnameFromVMId(vmId);
        if(hostname == null)
            return false;
        expireAllLeases(hostname);
        return true;
    }

    /**
     * Expire all leases currently stored.
     */
    public void expireAllLeases() {
        logger.info("Expiring all leases");
        assignableVMs.expireAllLeases();
    }

    /**
     * Get the task assigner action object.
     * Tasks are scheduled in {@code scheduleOnce()} but not tracked by this class. Tracking assigned tasks
     * is useful for optimizing future assignments for such purposes as task locality with other tasks, etc. If such
     * optimization is desired, call the taskAssigner from this method once for each task assignment actually used. 
     * Later, when that task terminates, call the un-assigner from {@code getTaskUnAssigner()} as well.
     * 
     * Note that calling the task assigner action concurrently with {@code scheduleOnce()} is disallowed. The
     * task assigner action will throw {@code IllegalStateException} in such a case.
     * 
     * @return Task assigner action
     */
    public Action2<TaskRequest, String> getTaskAssigner() {
        return new Action2<TaskRequest, String>() {
            @Override
            public void call(TaskRequest request, String hostname) {
                try (AutoCloseable ac = stateMonitor.enter()) {
                    assignableVMs.setTaskAssigned(request, hostname);
                } catch (Exception e) {
                    logger.error("Unexpected error from state monitor: " + e.getMessage());
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    /**
     * Get the task unassigner object.
     * Call this object's {@code call()} method to unassign a previously set assignment for 
     * each task that completes so internal state is maitained correctly.
     * 
     * @return The task un-assigner action.
     */
    public Action2<String, String> getTaskUnAssigner() {
        return new Action2<String, String>() {
            @Override
            public void call(String taskId, String hostname) {
                assignableVMs.unAssignTask(taskId, hostname);
            }
        };
    }

    /**
     * Disable a VM with the given hostname. If the hostname is not known yet, a new object for it is created and
     * therefore, the disabling is remembered when offers come in later. Disabled hosts are not used for allocating
     * resources to tasks.
     * 
     * @param hostname Name of the host to disable.
     * @param durationMillis duration, in mSec, from now until which to disable
     */
    public void disableVM(String hostname, long durationMillis) {
        logger.info("Disable VM " + hostname + " for " + durationMillis + " millis");
        assignableVMs.disableUntil(hostname, System.currentTimeMillis()+durationMillis);
    }



    /**
     * Disable a VM given it's ID.
     * 
     * @param vmID The VM ID
     * @param durationMillis duration, in mSec, from now until which to disable
     * @return {@code true} if VM ID was known, {@code false} otherwise.
     */
    public boolean disableVMByVMId(String vmID, long durationMillis) {
        final String hostname = assignableVMs.getHostnameFromVMId(vmID);
        if(hostname == null)
            return false;
        disableVM(hostname, durationMillis);
        return true;
    }

    /**
     * Enable the VM with the given host name.
     * @param hostname Name of the host.
     */
    public void enableVM(String hostname) {
        logger.info("Enabling VM " + hostname);
        assignableVMs.enableVM(hostname);
    }

    /**
     * A VM (host) can belong to a group. The group is determine by the value of the given attribute name in its offers.
     * @param attributeName Name of the attribute to determine a VM's group.
     */
    public void setActiveVmGroupAttributeName(String attributeName) {
        assignableVMs.setActiveVmGroupAttributeName(attributeName);
    }

    /**
     * Set the list of VM group names that are active. VMs (hosts) that belong to groups not included in this list are
     * said to be disabled. Disabled hosts' resources are not used for allocation. A null list indicates that all
     * groups are enabled.
     * @param vmGroups List of VM group names that are set as enabled.
     */
    public void setActiveVmGroups(List<String> vmGroups) {
        assignableVMs.setActiveVmGroups(vmGroups);
    }

}
