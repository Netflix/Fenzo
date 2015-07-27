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
 * A scheduling service for assigning resources to tasks. Call the method {@link #scheduleOnce scheduleOnce()}
 * with a list of task requests and a list of new resource lease offers. The {@code TaskScheduler} stores any
 * unused lease offers and will apply them during future calls to {@code scheduleOnce()} until a time expires,
 * which is defined by the lease offer expiry time that you set when you build the {@code TaskScheduler} (the
 * default is 10 seconds). Upon reaching the expiry time, the {@code TaskScheduler} rejects resource lease offers
 * by invoking the action supplied with the builder.
 * <p>
 * TaskScheduler can be used in two modes:
 * <ol>
 *  <li><b>Simple mode with no optimizations:</b> In this mode, after you build the {@code TaskScheduler} object,
 *      you only need to call the {@code scheduleOnce()} method.</li>
 *  <li><b>Optimizations mode:</b> In this mode, {@code TaskScheduler} attempts to optimize the placement of
 *      tasks on resources by using optimization functions (to be added). This requires that you not only call
 *      the {@code scheduleOnce()} method but also the task assigner and task un-assigner actions available
 *      from the methods {@link #getTaskAssigner getTaskAssigner()} and
 *      {@link #getTaskUnAssigner getTaskUnAssigner()}. These actions make the {@code TaskScheduler} keep track
 *      of tasks already assigned. The {@code TaskScheduler} then makes these tracked tasks available to
 *      the optimization functions.</li>
 * </ol>
 * Do not call the scheduler concurrently. The scheduler assigns tasks in the order that they are received in a
 * particular list. It checks each task against available resources until it finds a match.
 * <p>
 * You create your {@code TaskScheduler} by means of the {@link TaskScheduler.Builder}. It provide methods with
 * which you can adjust the scheduler's autoscaling rules, fitness calculators, and so forth.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Builder_pattern">Wikipedia: Builder pattern</a>
 */
public class TaskScheduler {

    private static final int PARALLEL_SCHED_EVAL_MIN_BATCH_SIZE = 30;

    /**
     * The Builder is how you construct a {@link TaskScheduler} object with particular characteristics. Chain
     * its methods and then call {@link #build build()} to create a {@code TaskScheduler}.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Builder_pattern">Wikipedia: Builder pattern</a>
     */
    public final static class Builder {

        private Action1<VirtualMachineLease> leaseRejectAction=null;
        private long leaseOfferExpirySecs=120;
        private VMTaskFitnessCalculator fitnessCalculator = new DefaultFitnessCalculator();
        private String autoScaleByAttributeName=null;
        private String autoScalerMapHostnameAttributeName=null;
        private String autoScaleDownBalancedByAttributeName=null;
        private Action1<AutoScaleAction> autoscalerCallback=null;
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
         * Invoke the given action when rejecting a VM lease.
         * @param leaseRejectAction The single argument action to trigger when rejecting a VM lease, with the lease
         *                          being rejected as the only argument.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withLeaseRejectAction(Action1<VirtualMachineLease> leaseRejectAction) {
            this.leaseRejectAction = leaseRejectAction;
            return this;
        }

        /**
         * Set the expiration time for lease offers after they have been received but remain unused by current
         * task requests.
         *
         * @param leaseOfferExpirySecs the amount of time the scheduler will keep an unused lease available for
         *        a later-scheduled task before it considers the lease to have expired, in seconds
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withLeaseOfferExpirySecs(long leaseOfferExpirySecs) {
            this.leaseOfferExpirySecs = leaseOfferExpirySecs;
            return this;
        }

        /**
         * Adds a fitness calculator that the scheduler will use to compute the suitability of a particular
         * target for a particular task. You can only add a single fitness calculator to a scheduler; if you
         * attempt to add a second fitness calculator, it will override the first one.
         *
         * @param fitnessCalculator the fitness calculator you want this scheduler to use in its evaluations
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withFitnessCalculator(VMTaskFitnessCalculator fitnessCalculator) {
            this.fitnessCalculator = fitnessCalculator;
            return this;
        }

        /**
         * Indicate which target attribute you want this scheduler to use in order to identify which targets are
         * in which autoscaling groups.
         *
         * @param name the name of the target attribute that defines which autoscaling group it is in
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withAutoScaleByAttributeName(String name) {
            this.autoScaleByAttributeName = name;
            return this;
        }

        /**
         * Use the given attribute name to determine alternate hostname of VM to use as argument for autoscaling action
         * on the VM.
         *
         * @param name Attribute name.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withAutoScalerMapHostnameAttributeName(String name) {
            this.autoScalerMapHostnameAttributeName = name;
            return this;
        }

        /**
         * When scaling down cluster, balance the number of VMs across unique values of the given attribute name.
         *
         * @param name Attribute name.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withAutoScaleDownBalancedByAttributeName(String name) {
            this.autoScaleDownBalancedByAttributeName = name;
            return this;
        }

        /**
         * Use the given function to determine if obtained fitness is good enough. If this is not provided, Fenzo
         * may use the default function that returns true only when fitness is {@code 1.0} (perfect fit).
         *
         * @param f Single argument function that acceps a double value, the fitness, and returns a {@code Boolean}.
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withFitnessGoodEnoughFunction(Func1<Double, Boolean> f) {
            this.isFitnessGoodEnoughFunction = f;
            return this;
        }

        /**
         * Disable resource shortfall evaluation. The shortfall evaluation is performed when evaluating the autoscaling
         * needs. This is useful for evaluating the actual resources needed to scale up by, for pending tasks, which may
         * be greater than the number of resources scaled up by thresholds based scale up.
         *
         * This evaluation can be computaionally expensive and/or may scale up aggressively, initially, to more resources
         * than needed. The initial aggressive scale up is corrected later by scale down, which is triggered by scale
         * down evaluation after a cool down period transpires.
         *
         * @see AutoScaleRule
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder disableShortfallEvaluation() {
            disableShortfallEvaluation = true;
            return this;
        }

        /**
         * Initialize the scheduler with the given mapping of resource allocation limits.
         *
         * @param resAllocs Map with task group name as keys and resource allocation limits as values.
         *                  @see ResAllocs
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         */
        public Builder withInitialResAllocs(Map<String, ResAllocs> resAllocs) {
            this.resAllocs = resAllocs;
            return this;
        }

        /**
         * Adds an autoscaling rule that governs the behavior by which this scheduler will autoscale hosts of a
         * certain type. You can chain this method multiple times, adding a new autoscaling rule each time.
         * Before you call this method you must first call
         * {@link #withAutoScaleByAttributeName withAutoScaleByAttributeName()} to indicate which target
         * attribute you are using to identify which targets are in which autoscaling groups.
         *
         * @param rule the autoscaling rule to add
         * @return this same {@code Builder}, suitable for further chaining or to build the {@link TaskScheduler}
         * @throws IllegalArgumentException if you have not properly initialized autoscaling or if your rule is
         *         poorly formed
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

        public Builder withAutoScalerCallback(Action1<AutoScaleAction> callback) {
            this.autoscalerCallback = callback;
            return this;
        }

        /**
         * Creates a {@link TaskScheduler} based on the various builder methods you have chained.
         *
         * @return a {@code TaskScheduler} built according to the specifications you indicated
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
            if(builder.autoscalerCallback != null)
                autoScaler.setCallback(builder.autoscalerCallback);
        }
        else {
            autoScaler=null;
        }
    }

    /**
     * Set the autoscale call back action. The scheduler calls this action when it determines that the cluster
     * needs to be scaled up or down.
     *
     * @param callback the callback to invoke for autoscale actions
     * @throws IllegalStateException if no autoscaler was established
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
     * Get the current mapping of resource allocations registered with the scheduler.
     *
     * @return current mapping of resource allocations
     */
    public Map<String, ResAllocs> getResAllocs() {
        return resAllocsEvaluator.getResAllocs();
    }

    /**
     * Add a new resource allocation, or replace an existing one of the same name.
     *
     * @param resAllocs the resource allocation to add or replace
     */
    public void addOrReplaceResAllocs(ResAllocs resAllocs) {
        resAllocsEvaluator.replaceResAllocs(resAllocs);
    }

    /**
     * Remove a resource allocation associated with the specified name.
     *
     * @param groupName the name of the resource allocation to remove
     */
    public void removeResAllocs(String groupName) {
        resAllocsEvaluator.remResAllocs(groupName);
    }

    /**
     * Get the autoscale rules currently registered with the scheduler.
     *
     * @return a collection of currently registered autoscale rules
     */
    public Collection<AutoScaleRule> getAutoScaleRules() {
        if(autoScaler==null)
            return Collections.emptyList();
        return autoScaler.getRules();
    }

    /**
     * Add a new autoscale rule to those used by this scheduler. If a rule with the same name exists, it is
     * replaced. This autoscale rule will be used the next time the scheduler invokes its autoscale action.
     *
     * @param rule the autoscale rule to add
     */
    public void addOrReplaceAutoScaleRule(AutoScaleRule rule) {
        autoScaler.replaceRule(rule);
    }

    /**
     * Remove the autoscale rule associated with the given name from those used by the scheduler.
     *
     * @param ruleName name of the autoscale rule to remove
     */
    public void removeAutoScaleRule(String ruleName) {
        autoScaler.removeRule(ruleName);
    }

    /**
     * Schedule a list of task requests by using any newly-added resource leases in addition to any
     * previously-unused leases. This is the main scheduling method that attempts to assign resources to task
     * requests. Resource leases are associated with a host name. A host can have zero or more leases. Leases
     * that the scheduler does not use in this scheduling run it stores for later use until they expire.
     * <p>
     * If you attempt to add a lease object with an Id equal to that of a stored lease object is disallowed, and
     * {@code scheduleOnce()} will throw an {@code IllegalStateException}. Upon throwing this exception, if you
     * provided multiple leases in the {@code newLeases} argument, the state of internally maintained list of
     * unused leases will be in an indeterminate state - some of the leases may have been successfully added.
     * <p>
     * Any expired leases are rejected before scheduling begins. Then, all leases of a host are combined to
     * determine total available resources on the host. Each task request, in the order that they appear in
     * the given list, is then tried for assignment against the available hosts until successful. For each
     * task, either a successful assignment result, or, the set of assignment failures, is returned.
     * <p>
     * After the scheduler evaluates all assignments, it will reject remaining leases if they are unused and
     * their offer time is further in the past than lease expiration interval. This prevents the scheduler from
     * hoarding leases. If you provided an autoscaler, the scheduler then launches autoscale evaluation to run
     * asynchronously, which runs each registered autoscale rule based on its policy.
     * 
     * @param requests a list of task requests to match with resources, in their given order
     * @param newLeases new resource leases from hosts that the scheduler can use along with any previously
     *                  ununsed leases
     * @return a {@link SchedulingResult} object that contains a task assignment results map and other summaries
     * @throws IllegalStateException if you call this method concurrently or if you try to readd an existing
     *         lease
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
     * Returns the current state of all known hosts. You might occasionally use this for debugging or
     * informational purposes. If you call this method, it will obtain and hold a lock for as long as it takes to
     * create the state information. Scheduling runs are blocked around the lock.
     * 
     * @return a list containing the current state of all known VMs
     * @throws IllegalStateException if you call this concurrently with the main scheduling method,
     *         {@link #scheduleOnce}
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
     * Expire a resource lease with the given lease ID.
     *
     * @param leaseId the lease ID of the lease to expire
     */
    public void expireLease(String leaseId) {
        assignableVMs.expireLease(leaseId);
    }

    /**
     * Expire all leases of the host with the name {@code hostname}.
     *
     * @param hostname the name of the host whose leases you want to expire
     */
    public void expireAllLeases(String hostname) {
        assignableVMs.expireAllLeases(hostname);
    }

    /**
     * Expire all leases of the host with the ID, {@code vmId}.
     *
     * @param vmId the ID of the host whose leases you want to expire
     * @return {@code true} if the given ID matches a known host, {@code false} otherwise.
     */
    public boolean expireAllLeasesByVMId(String vmId) {
        final String hostname = assignableVMs.getHostnameFromVMId(vmId);
        if(hostname == null)
            return false;
        expireAllLeases(hostname);
        return true;
    }

    /**
     * Expire all leases currently stored by the scheduler.
     */
    public void expireAllLeases() {
        logger.info("Expiring all leases");
        assignableVMs.expireAllLeases();
    }

    /**
     * Get the task assigner action. Tasks are scheduled by {@link #scheduleOnce scheduleOnce()} but are
     * not tracked by this class. Tracking assigned tasks is useful for optimizing future assignments for such
     * purposes as task locality with other tasks, and so forth. If you desire such optimization, call the
     * task assigner returned from this method once for each task assignment actually used. Later, when that task
     * terminates, call the un-assigner from {@link #getTaskUnAssigner getTaskUnAssigner()} as well.
     * <p>
     * Note that you may not call the task assigner action concurrently with {@code scheduleOnce()}. If you do
     * so, the task assigner action will throw an {@code IllegalStateException}.
     * 
     * @return a task assigner action
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
     * Get the task unassigner action. Call this object's {@code call()} method to unassign an assignment you
     * have previously set for each task that completes so that internal state is maintained correctly.
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
     * Disable a VM with the specified hostname. If the scheduler is not aware of the hostname yet, it creates a
     * new object for it, and therefore your disabling of it will be remembered when offers come in later.
     * The scheduler will not use disabled hosts for allocating resources to tasks.
     * 
     * @param hostname the name of the host to disable
     * @param durationMillis the length of time, starting from now, in milliseconds, during which the host will
     *        be disabled
     */
    public void disableVM(String hostname, long durationMillis) {
        logger.info("Disable VM " + hostname + " for " + durationMillis + " millis");
        assignableVMs.disableUntil(hostname, System.currentTimeMillis()+durationMillis);
    }



    /**
     * Disable the VM with the specified ID.
     * 
     * @param vmID the VM ID
     * @param durationMillis the length of time, starting from now, in milliseconds, during which the host will
     *        be disabled
     * @return {@code true} if the ID matches a known VM, {@code false} otherwise.
     */
    public boolean disableVMByVMId(String vmID, long durationMillis) {
        final String hostname = assignableVMs.getHostnameFromVMId(vmID);
        if(hostname == null)
            return false;
        disableVM(hostname, durationMillis);
        return true;
    }

    /**
     * Enable the VM with the specified host name.
     *
     * @param hostname the name of the host
     */
    public void enableVM(String hostname) {
        logger.info("Enabling VM " + hostname);
        assignableVMs.enableVM(hostname);
    }

    /**
     * Set how the scheduler determines to which group the VM (host) belongs. You can group hosts. Which group a
     * host belongs to is determined by the value of a particular attribute in its offers. You can set which
     * attribute defines group membership by naming it in this method.
     *
     * @param attributeName the name of the attribute that determines a VM's group
     */
    public void setActiveVmGroupAttributeName(String attributeName) {
        assignableVMs.setActiveVmGroupAttributeName(attributeName);
    }

    /**
     * Set the list of VM group names that are active. VMs (hosts) that belong to groups that you do not include
     * in this list are said to be disabled. The scheduler does not use the resources of disabled hosts when it
     * allocates tasks. If you pass in a null list, this indicates that the scheduler should consider all groups
     * to be enabled.
     *
     * @param vmGroups a list of VM group names that the scheduler is to consider to be enabled, or {@code null}
     *        if the scheduler is to consider every group to be enabled
     */
    public void setActiveVmGroups(List<String> vmGroups) {
        assignableVMs.setActiveVmGroups(vmGroups);
    }

}
